use clap::Parser;

use dropout::Dropper;
use indicatif_log_bridge::LogWrapper;
use jbk::creator::{BasicCreator, CompHint, ConcatMode, InputReader};
use mime_guess::{mime, Mime};
use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
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

pub struct Converter {
    basic_creator: BasicCreator,
    entry_store_creator: Box<waj::create::EntryStoreCreator>,
    progress: Arc<ProgressBar>,
    has_main_page: bool,
    dropper: Dropper<Droppable>,
}

enum ZimEntryKind {
    Redirect(String),
    Content(jbk::ContentAddress, Mime),
}

struct ZimEntry {
    path: String,
    data: ZimEntryKind,
}

impl ZimEntry {
    pub fn new(
        entry: zim_rs::entry::Entry,
        dropper: &Dropper<Droppable>,
        adder: &mut BasicCreator,
    ) -> jbk::Result<Self> {
        let path = entry.get_path();
        let path = path.strip_prefix('/').unwrap_or(&path);
        Ok(if entry.is_redirect() {
            Self::new_redirect(path.into(), entry.get_redirect_entry().unwrap().get_path())
        } else {
            let item = entry.get_item(false).unwrap();
            dropper.dropout(entry.into());
            let item_mimetype = item.get_mimetype().unwrap();
            let item_size = item.get_size();
            let direct_access = item.get_direct_access().unwrap();
            let comp_hint = if direct_access.is_some() {
                CompHint::No
            } else {
                CompHint::Yes
            };
            let reader: Box<dyn InputReader> =
                if direct_access.is_none() || item_size <= 4 * 1024 * 1024 {
                    Box::new(std::io::Cursor::new(item.get_data().unwrap()))
                } else {
                    let direct_access = direct_access.unwrap();
                    Box::new(jbk::creator::InputFile::new_range(
                        std::fs::File::open(direct_access.get_path())?,
                        direct_access.get_offset(),
                        Some(item_size),
                    )?)
                };
            let content_address = adder.add_content(reader, comp_hint)?;
            dropper.dropout(item.into());
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
    pub fn new_redirect(path: String, target: String) -> Self {
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

    fn name(&self) -> Cow<str> {
        Cow::Borrowed(&self.path)
    }
}

#[allow(dead_code)]
enum Droppable {
    Blob(zim_rs::blob::Blob),
    Entry(zim_rs::entry::Entry),
    Item(zim_rs::item::Item),
}

impl From<zim_rs::blob::Blob> for Droppable {
    fn from(value: zim_rs::blob::Blob) -> Self {
        Self::Blob(value)
    }
}
impl From<zim_rs::entry::Entry> for Droppable {
    fn from(value: zim_rs::entry::Entry) -> Self {
        Self::Entry(value)
    }
}
impl From<zim_rs::item::Item> for Droppable {
    fn from(value: zim_rs::item::Item) -> Self {
        Self::Item(value)
    }
}

impl Converter {
    pub fn new<P: AsRef<Path>>(
        zim: &Archive,
        outfile: P,
        concat_mode: ConcatMode,
    ) -> jbk::Result<Self> {
        let progress = Arc::new(ProgressBar::new(zim)?);
        let basic_creator = BasicCreator::new(
            &outfile,
            concat_mode,
            waj::VENDOR_ID,
            jbk::creator::Compression::zstd(),
            Arc::clone(&progress) as Arc<dyn jbk::creator::Progress>,
        )?;
        let entry_store_creator = Box::new(waj::create::EntryStoreCreator::new(Some(
            zim.get_all_entrycount() as usize,
        )));

        Ok(Self {
            basic_creator,
            entry_store_creator,
            progress,
            has_main_page: false,
            dropper: Dropper::new(),
        })
    }

    fn finalize(self, outfile: &Path) -> jbk::Result<()> {
        self.basic_creator
            .finalize(outfile, self.entry_store_creator, vec![])
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
            let entry = ZimEntry::new_redirect("".into(), main_page_path);
            self.entry_store_creator.add_entry(&entry)?;
        }

        self.finalize(&outfile)
    }

    fn handle(&mut self, entry: zim_rs::entry::Entry) -> jbk::Result<()> {
        self.progress.entries.inc(1);
        if entry.get_path().is_empty() {
            self.has_main_page = true;
        }

        let entry = ZimEntry::new(entry, &self.dropper, &mut self.basic_creator)?;
        self.entry_store_creator.add_entry(&entry)
    }
}

fn main() -> jbk::Result<()> {
    let args = Cli::parse();

    let zim = Archive::new(args.zim_file.to_str().unwrap()).unwrap();
    let converter = Converter::new(&zim, &args.outfile, ConcatMode::OneFile)?;
    converter.run(&zim, args.outfile)
}
