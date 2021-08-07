use simple_db::db::blockid::BlockId;

#[test]
fn test_display() {
    let filename = String::from("test");
    let blknum = 10;

    let bi = BlockId::new(&filename, blknum);

    assert_eq!(filename, bi.filename());
    assert_eq!(blknum, bi.number());
    assert_eq!(
        format!("[file {}, block {}]", filename, blknum),
        format!("{}", bi)
    )
}
