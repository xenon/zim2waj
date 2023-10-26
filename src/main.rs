use jubako as jbk;

use clap::Parser;

use indicatif_log_bridge::LogWrapper;
use jbk::creator::OutStream;
use mime_guess::{mime, Mime};
use std::ffi::OsStr;
use std::ffi::OsString;
use std::io::{self, Read, Seek, Write};
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
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
    pub written_clusters: indicatif::ProgressBar,
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
            .format_timestamp_millis()
            .build();

        let draw_target = indicatif::ProgressDrawTarget::stdout_with_hz(1);

        let multi = indicatif::MultiProgress::with_draw_target(draw_target);
        multi.set_move_cursor(true);

        let nb_entries = Self::gather_information(zim);

        let bytes_style = indicatif::ProgressStyle::with_template(
            "{prefix} : {bytes:7} ({binary_bytes_per_sec})",
        )
        .unwrap();
        let size = indicatif::ProgressBar::new_spinner()
            .with_style(bytes_style)
            .with_prefix("Processed size");
        multi.add(size.clone());

        let cluster_style =
            indicatif::ProgressStyle::with_template("{prefix} : {human_pos} ({human_len})")
                .unwrap();
        let comp_clusters = indicatif::ProgressBar::new(0)
            .with_style(cluster_style.clone())
            .with_prefix("Compressed Cluster");

        let uncomp_clusters = indicatif::ProgressBar::new(0)
            .with_style(cluster_style.clone())
            .with_prefix("Uncompressed Cluster");

        let written_clusters = indicatif::ProgressBar::new(0)
            .with_style(cluster_style.clone())
            .with_prefix("Written clusters");
        multi.add(comp_clusters.clone());
        multi.add(uncomp_clusters.clone());
        multi.add(written_clusters.clone());

        let entries_style = indicatif::ProgressStyle::with_template(
                "{prefix} : {elapsed} / {duration} : [{wide_bar:.cyan/blue}] {human_pos:10} / {human_len:10}"
            )
            .unwrap()
            .progress_chars("#+- ");
        let entries = indicatif::ProgressBar::new(nb_entries as u64)
            .with_style(entries_style)
            .with_prefix("Processed entries");
        multi.add(entries.clone());

        comp_clusters.tick();
        uncomp_clusters.tick();
        written_clusters.tick();

        LogWrapper::new(multi.clone(), logger).try_init().unwrap();
        Ok(Self {
            entries,
            comp_clusters,
            uncomp_clusters,
            written_clusters,
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

    fn handle_cluster_written(&self, _cluster_idx: u32) {
        self.written_clusters.inc(1)
    }

    fn content_added(&self, size: jbk::Size) {
        self.size.inc(size.into_u64())
    }
}

pub struct ContentAdder<O: OutStream + 'static> {
    content_pack: jbk::creator::ContentPackCreator<O>,
}

impl<O: OutStream + 'static> ContentAdder<O> {
    fn new(content_pack: jbk::creator::ContentPackCreator<O>) -> Self {
        Self { content_pack }
    }

    fn into_inner(self) -> jbk::creator::ContentPackCreator<O> {
        self.content_pack
    }
}

impl<O: OutStream + 'static> waj::create::Adder for ContentAdder<O> {
    fn add<R: jbk::creator::InputReader>(&mut self, reader: R) -> jbk::Result<jbk::ContentAddress> {
        let content_id = self.content_pack.add_content(reader)?;
        Ok(jbk::ContentAddress::new(1.into(), content_id))
    }
}

#[derive(Debug)]
pub enum MaybeInContainer {
    In(jbk::creator::InContainerFile),
    No(std::fs::File),
}

impl OutStream for MaybeInContainer {
    fn copy(&mut self, reader: Box<dyn jbk::creator::InputReader>) -> io::Result<u64> {
        match self {
            Self::In(f) => f.copy(reader),
            Self::No(f) => f.copy(reader),
        }
    }
}

impl Seek for MaybeInContainer {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        match self {
            Self::In(f) => f.seek(pos),
            Self::No(f) => f.seek(pos),
        }
    }
}

impl Write for MaybeInContainer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::In(f) => f.write(buf),
            Self::No(f) => f.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::In(f) => f.flush(),
            Self::No(f) => f.flush(),
        }
    }
}

impl Read for MaybeInContainer {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::In(f) => f.read(buf),
            Self::No(f) => f.read(buf),
        }
    }
}

pub struct Converter {
    adder: ContentAdder<MaybeInContainer>,
    directory_pack: jbk::creator::DirectoryPackCreator,
    entry_store_creator: waj::create::EntryStoreCreator,
    concat_mode: ConcatMode,
    tmp_path_content_pack: tempfile::TempPath,
    out_dir: PathBuf,
    progress: Arc<ProgressBar>,
    has_main_page: bool,
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
    pub fn new<O>(entry: zim_rs::entry::Entry, adder: &mut ContentAdder<O>) -> jbk::Result<Self>
    where
        O: OutStream + 'static,
    {
        let path = entry.get_path();
        let path = path.strip_prefix('/').unwrap_or(&path);
        Ok(if entry.is_redirect() {
            Self::new_redirect(
                path.into(),
                entry.get_redirect_entry().unwrap().get_path().into(),
            )
        } else {
            let item = entry.get_item(false).unwrap();
            let item_mimetype = item.get_mimetype().unwrap();
            let item_size = item.get_size();
            let direct_access = item.get_direct_access().unwrap();
            let content_address = if direct_access.is_none() || item_size <= 4 * 1024 * 1024 {
                let blob_reader = std::io::Cursor::new(item.get_data().unwrap());
                adder.add(blob_reader)?
            } else {
                let direct_access = direct_access.unwrap();
                let reader = jbk::creator::InputFile::new_range(
                    std::fs::File::open(direct_access.get_path())?,
                    direct_access.get_offset(),
                    Some(item_size),
                )?;
                adder.add(reader)?
            };
            Self {
                path: path.into(),
                data: ZimEntryKind::Content(
                    content_address,
                    Mime::from_str(&item_mimetype).unwrap_or({
                        /*println!(
                            "{}: {} is not a valid mime type. Using mime::APPLICATION_OCTET_STREAM",
                            entry.get_path(),
                            &item_mimetype
                        );*/
                        mime::APPLICATION_OCTET_STREAM
                    }),
                ),
            }
        })
    }
    pub fn new_redirect(path: OsString, target: OsString) -> Self {
        Self {
            path,
            data: ZimEntryKind::Redirect(target),
        }
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
        zim: &Archive,
        outfile: P,
        concat_mode: ConcatMode,
    ) -> jbk::Result<Self> {
        let outfile = outfile.as_ref();
        let out_dir = outfile.parent().unwrap().to_path_buf();

        let progress = Arc::new(ProgressBar::new(zim)?);

        let (tmp_content_pack, tmp_path_content_pack) =
            tempfile::NamedTempFile::new_in(&out_dir)?.into_parts();

        let tmp_content_pack = if let ConcatMode::OneFile = concat_mode {
            MaybeInContainer::In(
                jbk::creator::ContainerPackCreator::from_file(tmp_content_pack)?.into_file()?,
            )
        } else {
            MaybeInContainer::No(tmp_content_pack)
        };
        let content_pack = jbk::creator::ContentPackCreator::new_from_output_with_progress(
            tmp_content_pack,
            jbk::PackId::from(1),
            VENDOR_ID,
            Default::default(),
            jbk::creator::Compression::zstd(),
            Arc::clone(&progress) as Arc<dyn jbk::creator::Progress>,
        )?;

        let directory_pack = jbk::creator::DirectoryPackCreator::new(
            jbk::PackId::from(0),
            VENDOR_ID,
            Default::default(),
        );

        let entry_store_creator =
            waj::create::EntryStoreCreator::new(Some(zim.get_all_entrycount() as usize));

        Ok(Self {
            adder: ContentAdder::new(content_pack),
            directory_pack,
            entry_store_creator,
            concat_mode,
            progress,
            tmp_path_content_pack,
            out_dir,
            has_main_page: false,
        })
    }

    fn finalize(mut self, outfile: &Path) -> jbk::Result<()> {
        self.entry_store_creator
            .finalize(&mut self.directory_pack)?;

        let (content_pack_file, content_pack_info) = self.adder.into_inner().finalize()?;

        let (mut container, content_locator) = match self.concat_mode {
            ConcatMode::OneFile => {
                // Our content_pack_file IS the container pack
                if let MaybeInContainer::In(tmp_file) = content_pack_file {
                    let container = tmp_file.close(content_pack_info.uuid)?;
                    if let Err(e) = self.tmp_path_content_pack.persist(outfile) {
                        return Err(e.error.into());
                    }
                    (Some(container), vec![])
                } else {
                    panic!("content_pack_file should be a \"InContainer\"");
                }
            }
            _ => {
                // No concat of the content pack, so we have to persist it.
                // But we may need to create our container for the directory pack
                let mut outfilename = outfile.file_name().unwrap().to_os_string();
                outfilename.push(".jbkc");
                let mut content_pack_path = PathBuf::new();
                content_pack_path.push(outfile);
                content_pack_path.set_file_name(&outfilename);

                if let Err(e) = self.tmp_path_content_pack.persist(&content_pack_path) {
                    return Err(e.error.into());
                }

                let container = if let ConcatMode::TwoFiles = self.concat_mode {
                    Some(jbk::creator::ContainerPackCreator::new(outfile)?)
                } else {
                    None
                };
                (container, outfilename.into_vec())
            }
        };

        let (directory_pack_info, directory_locator) = match self.concat_mode {
            ConcatMode::NoConcat => {
                let (mut tmpfile, tmpname) =
                    tempfile::NamedTempFile::new_in(&self.out_dir)?.into_parts();
                let directory_pack_info = self.directory_pack.finalize(&mut tmpfile)?;

                let mut outfilename = outfile.file_name().unwrap().to_os_string();
                outfilename.push(".jbkd");
                let mut directory_pack_path = PathBuf::new();
                directory_pack_path.push(outfile);
                directory_pack_path.set_file_name(&outfilename);

                if let Err(e) = tmpname.persist(directory_pack_path) {
                    return Err(e.error.into());
                };
                (directory_pack_info, outfilename.into_vec())
            }
            _ => {
                let mut infile = container.unwrap().into_file()?;
                let directory_pack_info = self.directory_pack.finalize(&mut infile)?;
                container = Some(infile.close(directory_pack_info.uuid)?);
                (directory_pack_info, vec![])
            }
        };

        let mut manifest_creator =
            jbk::creator::ManifestPackCreator::new(VENDOR_ID, Default::default());

        manifest_creator.add_pack(directory_pack_info, directory_locator);
        manifest_creator.add_pack(content_pack_info, content_locator);

        match self.concat_mode {
            ConcatMode::NoConcat => {
                let (mut tmpfile, tmpname) =
                    tempfile::NamedTempFile::new_in(self.out_dir)?.into_parts();
                manifest_creator.finalize(&mut tmpfile)?;

                if let Err(e) = tmpname.persist(outfile) {
                    return Err(e.error.into());
                };
            }
            _ => {
                let mut infile = container.unwrap().into_file()?;
                let manifest_uuid = manifest_creator.finalize(&mut infile)?;
                container = Some(infile.close(manifest_uuid)?);
            }
        };
        if let Some(container) = container {
            container.finalize()
        } else {
            Ok(())
        }
    }

    pub fn run(mut self, zim: &Archive, outfile: PathBuf) -> jbk::Result<()> {
        info!(
            "Converting zim file with {} entries",
            zim.get_all_entrycount()
        );

        let iter = zim.iter_efficient().unwrap();
        let filter = if zim.has_new_namespace_scheme() {
            |_p: &str| true
        } else {
            |p: &str| matches!(&p.as_bytes()[0], b'-' | b'A' | b'C' | b'J' | b'I')
        };
        iter.into_iter()
            .map(|e| e.unwrap())
            .filter(|e| filter(&e.get_path()))
            .try_for_each(|e| self.handle(e))?;

        if !self.has_main_page {
            let main_page = zim.get_mainentry().unwrap();
            let main_page_path = main_page.get_item(true).unwrap().get_path();
            let entry = ZimEntry::new_redirect("".into(), main_page_path.into());
            self.entry_store_creator.add_entry(&entry)?;
        }

        self.finalize(&outfile)
    }

    fn handle(&mut self, entry: zim_rs::entry::Entry) -> jbk::Result<()> {
        self.progress.entries.inc(1);
        if entry.get_path().is_empty() {
            self.has_main_page = true;
        }

        let entry = ZimEntry::new(entry, &mut self.adder)?;
        self.entry_store_creator.add_entry(&entry)
    }
}

fn main() -> jbk::Result<()> {
    let args = Cli::parse();

    let zim = Archive::new(args.zim_file.to_str().unwrap()).unwrap();
    let converter = Converter::new(&zim, &args.outfile, ConcatMode::OneFile)?;
    converter.run(&zim, args.outfile)
}
