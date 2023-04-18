use jubako as jbk;

use clap::Parser;

use jbk::creator::schema;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use zim_rs::archive::Archive;

#[derive(Parser)]
#[clap(name = "zim2jim")]
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

pub struct Converter {
    content_pack: jbk::creator::ContentPackCreator,
    directory_pack: jbk::creator::DirectoryPackCreator,
    entry_store: Box<jbk::creator::EntryStore<Box<jbk::creator::BasicEntry>>>,
    entry_count: jbk::EntryCount,
    main_entry_id: Option<jbk::Bound<jbk::EntryIdx>>,
    zim: Archive,
}

impl Converter {
    pub fn new<P: AsRef<Path>>(infile: P, outfile: P) -> jbk::Result<Self> {
        let zim = Archive::new(infile.as_ref().to_str().unwrap()).unwrap();
        let outfile = outfile.as_ref();
        let mut outfilename: OsString = outfile.file_name().unwrap().to_os_string();
        outfilename.push(".jimc");
        let mut content_pack_path = PathBuf::new();
        content_pack_path.push(outfile);
        content_pack_path.set_file_name(outfilename);
        let content_pack = jbk::creator::ContentPackCreator::new(
            content_pack_path,
            jbk::PackId::from(1),
            VENDOR_ID,
            jbk::FreeData40::clone_from_slice(&[0x00; 40]),
            jbk::CompressionType::Zstd,
        )?;

        outfilename = outfile.file_name().unwrap().to_os_string();
        outfilename.push(".jimd");
        let mut directory_pack_path = PathBuf::new();
        directory_pack_path.push(outfile);
        directory_pack_path.set_file_name(outfilename);
        let mut directory_pack = jbk::creator::DirectoryPackCreator::new(
            directory_pack_path,
            jbk::PackId::from(0),
            VENDOR_ID,
            jbk::FreeData31::clone_from_slice(&[0x00; 31]),
        );

        let path_store = directory_pack.create_value_store(jbk::creator::ValueStoreKind::Plain);
        let mime_store = directory_pack.create_value_store(jbk::creator::ValueStoreKind::Indexed);

        let schema = schema::Schema::new(
            // Common part
            schema::CommonProperties::new(vec![
                schema::Property::new_array(1, Rc::clone(&path_store)), // the path
            ]),
            vec![
                // Content
                schema::VariantProperties::new(vec![
                    schema::Property::new_array(0, Rc::clone(&mime_store)), // the mimetype
                    schema::Property::new_content_address(),
                ]),
                // Redirect
                schema::VariantProperties::new(vec![
                    schema::Property::new_array(1, Rc::clone(&path_store)), // Id of the linked entry
                ]),
            ],
            Some(vec![0.into()]),
        );

        let entry_store = Box::new(jbk::creator::EntryStore::new(schema));

        Ok(Self {
            content_pack,
            directory_pack,
            entry_store,
            zim,
            entry_count: 0.into(),
            main_entry_id: None,
        })
    }

    fn finalize(mut self, outfile: PathBuf) -> jbk::Result<()> {
        let entry_store_id = self.directory_pack.add_entry_store(self.entry_store);
        self.directory_pack.create_index(
            "jim_entries",
            jubako::ContentAddress::new(0.into(), 0.into()),
            jbk::PropertyIdx::from(0),
            entry_store_id,
            self.entry_count,
            jubako::EntryIdx::from(0).into(),
        );
        self.directory_pack.create_index(
            "jim_main",
            jubako::ContentAddress::new(0.into(), 0.into()),
            jbk::PropertyIdx::from(0),
            entry_store_id,
            jubako::EntryCount::from(1),
            self.main_entry_id.unwrap().into(),
        );
        let directory_pack_info = self.directory_pack.finalize()?;
        let content_pack_info = self.content_pack.finalize()?;
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
        println!(
            "Converting zim file with {} entries",
            self.zim.get_all_entrycount()
        );
        let main_page = self.zim.get_mainentry().unwrap();
        println!("Main page is {}", main_page.get_path());
        let main_id = main_page.get_index();
        println!("Main id is {main_id}");
        let iter = self.zim.iter_efficient().unwrap();
        for entry in iter {
            let entry = entry.unwrap();
            let path = entry.get_path();
            match &path[0..1] {
                "-" | "A" | "C" | "J" | "I" => {
                    let is_main_entry = main_id == entry.get_index();
                    self.handle(entry, is_main_entry)?;
                }
                _ => {
                    println!("Skip {}", path);
                }
            }
        }
        self.finalize(outfile)
    }

    fn handle(&mut self, entry: zim_rs::entry::Entry, is_main_entry: bool) -> jbk::Result<()> {
        if self.entry_count.into_u32() % 1000 == 0 {
            println!("{} {:?}", self.entry_count, entry.get_path());
        }

        if entry.get_path() == "I/Postomat-Windows-p1020441.jpg.webp" {
            println!("======================================================================");
            println!("Entry is {}", entry.get_path());
        }

        let entry_idx = jbk::Vow::new(jbk::EntryIdx::from(0));
        let entry_idx_bind = entry_idx.bind();
        let entry = Box::new(if entry.is_redirect() {
            let mut entry_path = entry.get_path().into_bytes();
            entry_path.truncate(255);
            let entry_path = jbk::Value::Array(entry_path);

            let redirect_entry = entry.get_redirect_entry().unwrap();
            let mut target_path = redirect_entry.get_path().into_bytes();
            target_path.truncate(255);
            let target_path = jbk::Value::Array(target_path);

            jbk::creator::BasicEntry::new_from_schema_idx(
                &self.entry_store.schema,
                entry_idx,
                Some(1.into()),
                vec![entry_path, target_path],
            )
        } else {
            let mut entry_path = entry.get_path().into_bytes();
            entry_path.truncate(255);
            let entry_path = jbk::Value::Array(entry_path);

            let item = entry.get_item(false).unwrap();
            let item_size = item.get_size();
            let item_mimetype = item.get_mimetype().unwrap();
            let blob_reader = jbk::creator::Reader::new(
                item.get_data().unwrap(),
                jbk::End::Size(item_size.into()),
            );
            let content_id = self.content_pack.add_content(blob_reader)?;

            jbk::creator::BasicEntry::new_from_schema_idx(
                &self.entry_store.schema,
                entry_idx,
                Some(0.into()),
                vec![
                    entry_path,
                    jbk::Value::Array(item_mimetype.into()),
                    jbk::Value::Content(jbk::ContentAddress::new(jbk::PackId::from(1), content_id)),
                ],
            )
        });

        if is_main_entry {
            self.main_entry_id = Some(entry_idx_bind);
        }

        self.entry_store.add_entry(entry);
        self.entry_count += 1;
        Ok(())
    }
}

fn main() -> jbk::Result<()> {
    let args = Cli::parse();

    let converter = Converter::new(&args.zim_file, &args.outfile)?;
    converter.run(args.outfile)
}
