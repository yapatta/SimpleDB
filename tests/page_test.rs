use simple_db::db::page::Page;

#[test]
fn test_int() {
    let mut page = Page::new_from_size(20);
    let offset = 4;
    let value: i32 = 30;
    page.set_int(offset, value).unwrap();

    assert_eq!(30, page.get_int(offset).unwrap());
}

#[test]
fn test_string() {
    let mut page = Page::new_from_size(20);
    let offset = 4;
    let value = String::from("hello");
    page.set_string(offset, value).unwrap();

    assert_eq!("hello", page.get_string(offset).unwrap());
}
