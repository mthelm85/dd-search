use std::fs;
use std::io::{Cursor, stdout};

mod input;

use crossterm::style::{Color, Print, ResetColor, style, Stylize};
use crossterm::ExecutableCommand;
use crossterm::event::{read, Event, KeyCode, KeyEvent};
use crossterm::terminal;
use csv::Reader;
use structopt::StructOpt;
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::Index;
use tantivy::ReloadPolicy;

fn main() -> tantivy::Result<()> {
    let opt = input::Opt::from_args();

    // Define schema for the index
    let mut schema_builder = Schema::builder();
    let db_name = schema_builder.add_text_field("db_name", TEXT | STORED);
    let table_name = schema_builder.add_text_field("table_name", TEXT | STORED);
    let column_name = schema_builder.add_text_field("column_name", TEXT | STORED);
    let table_biz_desc = schema_builder.add_text_field("table_biz_desc", TEXT | STORED);
    let col_biz_desc = schema_builder.add_text_field("col_biz_desc", TEXT | STORED);
    let schema = schema_builder.build();

    let index_path = dirs::data_local_dir()
        .unwrap()
        .join("Data Dictionary Search Index");

    fs::create_dir_all(&index_path)?;

    let mmap_directory = MmapDirectory::open(&index_path)?;

    if !tantivy::Index::exists(&mmap_directory)? {
        // Create a new index
        let index = Index::create_in_dir(&index_path, schema.clone())?;

        // Open the CSV file
        let file = include_bytes!("D:\\Master Data Dictionary.csv");
        let cursor = Cursor::new(file);
        let mut rdr = Reader::from_reader(cursor);

        // Create a write lock
        let mut index_writer = index.writer(50_000_000)?;

        // Iterate over CSV records
        for result in rdr.records() {
            let record = result.unwrap();
            let rcrd_db_name = record.get(0).unwrap_or("");
            let rcrd_table_name = record.get(1).unwrap_or("");
            let rcrd_column_name = record.get(2).unwrap_or("");
            let rcrd_table_biz_desc = record.get(3).unwrap_or("");
            let rcrd_col_biz_desc = record.get(4).unwrap_or("");

            // Create a new document
            let mut doc = Document::new();
            doc.add_text(db_name, &rcrd_db_name);
            doc.add_text(table_name, &rcrd_table_name);
            doc.add_text(column_name, &rcrd_column_name);
            doc.add_text(table_biz_desc, &rcrd_table_biz_desc);
            doc.add_text(col_biz_desc, &rcrd_col_biz_desc);
            
            // Add the document to the index
            index_writer.add_document(doc)?;
        }

        // Commit changes to the index
        index_writer.commit()?;
    }

    let index = Index::open(MmapDirectory::open(&index_path).unwrap())?;

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()?;

    let searcher = reader.searcher();

    // Create a query parser
    let query_parser = QueryParser::for_index(&index, vec![db_name, table_name, column_name, table_biz_desc, col_biz_desc]);

    // Parse a query
    let query = query_parser.parse_query(&opt.search_term)?;

    // Create a top docs searcher
    let top_docs = searcher.search(&query, &TopDocs::with_limit(opt.record_limit))?;

    let database_field = schema.get_field("db_name").unwrap();
    let table_field = schema.get_field("table_name").unwrap();
    let column_field = schema.get_field("column_name").unwrap();
    let table_description_field = schema.get_field("table_biz_desc").unwrap();
    let column_description_field = schema.get_field("col_biz_desc").unwrap();

    let (terminal_width, _) = terminal::size()?;
    let separator = "-".repeat(terminal_width as usize);

    for (_score, doc_address) in top_docs {
        loop {
            match read()? {
                Event::Key(KeyEvent { code, .. }) => match code {
                    KeyCode::Enter => break,
                    KeyCode::Esc => return Ok(()),
                    _ => (),
                },
                _ => (),
            }
        }

        let retrieved_doc = searcher.doc(doc_address)?;
        let database_field = retrieved_doc.get_first(database_field).unwrap().as_text().unwrap();
        let table_field = retrieved_doc.get_first(table_field).unwrap().as_text().unwrap();
        let column_field = retrieved_doc.get_first(column_field).unwrap().as_text().unwrap();
        let table_description_field = retrieved_doc.get_first(table_description_field).unwrap().as_text().unwrap();
        let column_description_field = retrieved_doc.get_first(column_description_field).unwrap().as_text().unwrap();
    
        let database = style(database_field).with(Color::Magenta);
        let table = style(table_field).with(Color::Green);
        let column = style(column_field).with(Color::Blue);
        let table_description = style(table_description_field).with(Color::Green);
        let column_description = style(column_description_field).with(Color::Blue);
        let message = style("Press Enter to continue or Esc to exit...\n\n").with(Color::Yellow);
    
        println!("{}\n", separator);
        stdout().execute(Print(format!("Table: {} Column: {} Database: {}\n\n", table, column, database))).unwrap();
        stdout().execute(Print(format!("Table Description: {}\n\n", table_description))).unwrap();
        stdout().execute(Print(format!("Column Description: {}\n\n", column_description))).unwrap();
        stdout().execute(ResetColor).unwrap();
        stdout().execute(Print(message)).unwrap();

        loop {
            match read()? {
                Event::Key(KeyEvent { code, .. }) => match code {
                    KeyCode::Enter => break,
                    KeyCode::Esc => return Ok(()),
                    _ => (),
                },
                _ => (),
            }
        }

    }

    Ok(())
}
