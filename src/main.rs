use jubako as jbk;

use clap::Parser;

use indicatif_log_bridge::LogWrapper;
use mime_guess::{mime, Mime};
use std::ffi::OsStr;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use waj::create::Adder;
use zim_rs::archive::Archive;

use log::info;

#[derive(Parser)]
#[clap(name = "zim2waj")]
#[clap(author, version, about, long_about=None)]
struct Cli {
    // Input
    #[clap(value_parser)]
    zim_file: PathBuf,

    // Archive name to create
    #[clap(short, long, value_parser)]
    outfile: PathBuf,
}

const VENDOR_ID: u32 = 0x6a_69_6d_00;

pub enum ConcatMode {
    OneFile,
    TwoFiles,
    NoConcat,
}

#[derive(Clone)]
struct ProgressBar {
    pub comp_clusters: indicatif::ProgressBar,
    pub uncomp_clusters: indicatif::ProgressBar,
    pub entries: indicatif::ProgressBar,
    pub size: indicatif::ProgressBar,
}

impl ProgressBar {
    fn gather_information(zim: &Archive) -> u32 {
        zim.get_all_entrycount()
    }

    fn new(zim: &Archive) -> jbk::Result<Self> {
        let env = env_logger::Env::default()
            .filter("WAJ_LOG")
            .write_style("WAJ_LOG_STYLE");
        let logger = env_logger::Builder::from_env(env)
            .format_module_path(false)
            .format_timestamp(None)
            .build();
        let draw_target = indicatif::ProgressDrawTarget::stdout_with_hz(1);
        let style = indicatif::ProgressStyle::with_template(
            "{prefix} : [{wide_bar:.cyan/blue}] {pos:7} / {len:7}",
        )
        .unwrap()
        .progress_chars("#+- ");

        let multi = indicatif::MultiProgress::with_draw_target(draw_target);

        let nb_entries = Self::gather_information(zim);

        let bytes_style = indicatif::ProgressStyle::with_template(
            "{prefix} : {bytes:7} ({binary_bytes_per_sec})",
        )
        .unwrap();
        let size = indicatif::ProgressBar::new_spinner()
            .with_style(bytes_style)
            .with_prefix("Processed size");
        multi.add(size.clone());
        let comp_clusters = indicatif::ProgressBar::new(0)
            .with_style(style.clone())
            .with_prefix("Compressed Cluster  ");

        let uncomp_clusters = indicatif::ProgressBar::new(0)
            .with_style(style.clone())
            .with_prefix("Uncompressed Cluster");

        let entries_style = style
            .clone()
            .template("{elapsed} / {duration} : [{wide_bar:.cyan/blue}] {pos:7} / {len:7}")
            .unwrap();
        let entries = indicatif::ProgressBar::new(nb_entries as u64).with_style(entries_style);

            )
        multi.add(entries.clone());
        multi.add(size.clone());
        multi.add(comp_clusters.clone());
        multi.add(uncomp_clusters.clone());
        LogWrapper::new(multi.clone(), logger).try_init().unwrap();
        Ok(Self {
            entries,
            comp_clusters,
            uncomp_clusters,
            size,
        })
    }
}

impl jbk::creator::Progress for ProgressBar {
    fn new_cluster(&self, _cluster_idx: u32, compressed: bool) {
        if compressed {
            &self.comp_clusters
        } else {
            &self.uncomp_clusters
        }
        .inc_length(1)
    }
    fn handle_cluster(&self, _cluster_idx: u32, compressed: bool) {
        if compressed {
            &self.comp_clusters
        } else {
            &self.uncomp_clusters
        }
        .inc(1)
    }
    fn content_added(&self, size: jbk::Size) {
        self.size.inc(size.into_u64())
    }
}

pub struct ContentAdder {
    content_pack: jbk::creator::CachedContentPackCreator,
}

impl ContentAdder {
    fn new(content_pack: jbk::creator::CachedContentPackCreator) -> Self {
        Self { content_pack }
    }

    fn into_inner(self) -> jbk::creator::CachedContentPackCreator {
        self.content_pack
    }
}

impl waj::create::Adder for ContentAdder {
    fn add(&mut self, reader: jbk::Reader) -> jbk::Result<jbk::ContentAddress> {
        let content_id = self.content_pack.add_content(reader)?;
        Ok(jbk::ContentAddress::new(1.into(), content_id))
    }
}

pub struct Converter {
    adder: ContentAdder,
    directory_pack: jbk::creator::DirectoryPackCreator,
    entry_store_creator: waj::create::EntryStoreCreator,
    zim: Archive,
    concat_mode: ConcatMode,
    tmp_path_content_pack: tempfile::TempPath,
    tmp_path_directory_pack: tempfile::TempPath,
    progress: Arc<ProgressBar>,
}

enum ZimEntryKind {
    Redirect(OsString),
    Content(jbk::ContentAddress, Mime),
}

struct ZimEntry {
    path: OsString,
    data: ZimEntryKind,
}

impl ZimEntry {
    pub fn new(entry: zim_rs::entry::Entry, adder: &mut ContentAdder) -> jbk::Result<Self> {
        Ok(if entry.is_redirect() {
            Self {
                path: entry.get_path().into(),
                data: ZimEntryKind::Redirect(entry.get_redirect_entry().unwrap().get_path().into()),
            }
        } else {
            let item = entry.get_item(false).unwrap();
            let item_size = item.get_size();
            let item_mimetype = item.get_mimetype().unwrap();
            let blob_reader = jbk::creator::Reader::new(
                item.get_data().unwrap(),
                jbk::End::Size(item_size.into()),
            );
            let content_address = adder.add(blob_reader)?;
            Self {
                path: entry.get_path().into(),
                data: ZimEntryKind::Content(
                    content_address,
                    Mime::from_str(&item_mimetype).unwrap_or_else(|_e| {
                        println!(
                            "{} is not a valid mime type. Using mime::OCTET_STREAM",
                            &item_mimetype
                        );
                        mime::OCTET_STREAM
                    }),
                ),
            }
        })
    }
}

impl waj::create::EntryTrait for ZimEntry {
    fn kind(&self) -> jbk::Result<Option<waj::create::EntryKind>> {
        Ok(Some(match &self.data {
            ZimEntryKind::Redirect(target) => waj::create::EntryKind::Redirect(target.clone()),
            ZimEntryKind::Content(content_address, mime) => {
                waj::create::EntryKind::Content(*content_address, mime.clone())
            }
        }))
    }

    fn name(&self) -> &OsStr {
        &self.path
    }
}

impl Converter {
    pub fn new<P: AsRef<Path>>(
        infile: P,
        outfile: P,
        concat_mode: ConcatMode,
    ) -> jbk::Result<Self> {
        let zim = Archive::new(infile.as_ref().to_str().unwrap()).unwrap();
        let outfile = outfile.as_ref();
        let out_dir = outfile.parent().unwrap();

        let progress = Arc::new(ProgressBar::new(&zim)?);

        let (tmp_content_pack, tmp_path_content_pack) =
            tempfile::NamedTempFile::new_in(out_dir)?.into_parts();
        let content_pack = jbk::creator::ContentPackCreator::new_from_file_with_progress(
            tmp_content_pack,
            jbk::PackId::from(1),
            VENDOR_ID,
            jbk::FreeData40::clone_from_slice(&[0x00; 40]),
            jbk::CompressionType::Zstd,
            Arc::clone(&progress) as Arc<dyn jbk::creator::Progress>,
        )?;

        let (_, tmp_path_directory_pack) = tempfile::NamedTempFile::new_in(out_dir)?.into_parts();
        let directory_pack = jbk::creator::DirectoryPackCreator::new(
            &tmp_path_directory_pack,
            jbk::PackId::from(0),
            VENDOR_ID,
            jbk::FreeData31::clone_from_slice(&[0x00; 31]),
        );

        let main_page = zim.get_mainentry().unwrap();
        let main_path = main_page.get_item(true).unwrap().get_path();
        info!("Main page is {}", main_path);

        let entry_store_creator = waj::create::EntryStoreCreator::new(main_path.into());

        Ok(Self {
            adder: ContentAdder::new(jbk::creator::CachedContentPackCreator::new(
                content_pack,
                Rc::new(()),
            )),
            directory_pack,
            entry_store_creator,
            zim,
            concat_mode,
            progress,
            tmp_path_content_pack,
            tmp_path_directory_pack,
        })
    }

    fn finalize(mut self, outfile: PathBuf) -> jbk::Result<()> {
        self.entry_store_creator
            .finalize(&mut self.directory_pack)?;

        let directory_pack_info = match self.concat_mode {
            ConcatMode::NoConcat => {
                let mut outfilename = outfile.file_name().unwrap().to_os_string();
                outfilename.push(".jbkd");
                let mut directory_pack_path = PathBuf::new();
                directory_pack_path.push(&outfile);
                directory_pack_path.set_file_name(outfilename);
                let directory_pack_info = self
                    .directory_pack
                    .finalize(Some(directory_pack_path.clone()))?;
                if let Err(e) = self.tmp_path_directory_pack.persist(&directory_pack_path) {
                    return Err(e.error.into());
                };
                directory_pack_info
            }
            _ => self.directory_pack.finalize(None)?,
        };

        let content_pack_info = match self.concat_mode {
            ConcatMode::OneFile => self.adder.into_inner().into_inner().finalize(None)?,
            _ => {
                let mut outfilename = outfile.file_name().unwrap().to_os_string();
                outfilename.push(".jbkc");
                let mut content_pack_path = PathBuf::new();
                content_pack_path.push(&outfile);
                content_pack_path.set_file_name(outfilename);
                let content_pack_info = self
                    .adder
                    .into_inner()
                    .into_inner()
                    .finalize(Some(content_pack_path.clone()))?;
                if let Err(e) = self.tmp_path_content_pack.persist(&content_pack_path) {
                    return Err(e.error.into());
                }
                content_pack_info
            }
        };
        let mut manifest_creator = jbk::creator::ManifestPackCreator::new(
            outfile,
            VENDOR_ID,
            jbk::FreeData63::clone_from_slice(&[0x00; 63]),
        );

        manifest_creator.add_pack(directory_pack_info);
        manifest_creator.add_pack(content_pack_info);
        manifest_creator.finalize()?;
        Ok(())
    }

    pub fn run(mut self, outfile: PathBuf) -> jbk::Result<()> {
        info!(
            "Converting zim file with {} entries",
            self.zim.get_all_entrycount()
        );

        let iter = self.zim.iter_efficient().unwrap();
        let filter = if self.zim.has_new_namespace_scheme() {
            |_p: &str| true
        } else {
            |p: &str| match &p[0..1] {
                "-" | "A" | "C" | "J" | "I" => true,
                _ => false,
            }
        };
        for entry in iter {
            let entry = entry.unwrap();
            if filter(&entry.get_path()) {
                self.handle(entry)?;
            }
        }
        self.finalize(outfile)
    }

    fn handle(&mut self, entry: zim_rs::entry::Entry) -> jbk::Result<()> {
        self.progress.entries.inc(1);

        let entry = ZimEntry::new(entry, &mut self.adder)?;
        self.entry_store_creator.add_entry(&entry)
    }
}

fn main() -> jbk::Result<()> {
    let args = Cli::parse();

    let converter = Converter::new(&args.zim_file, &args.outfile, ConcatMode::OneFile)?;
    converter.run(args.outfile)
}
