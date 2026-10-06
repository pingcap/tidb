//! Multibyte positioning and lengths: LOCATE/INSTR report CHARACTER
//! positions ('a' after a 3-byte 中 is character 2, not byte 4), BIT_LENGTH
//! counts bits (32 for '中a'), and LPAD's target length is in characters
//! (lpad('中', 2, 'ab') = 'a中').

use tidb_session::Session;

use crate::support::try_tagged_rows_60 as try_sql;

#[test]
fn character_positions_not_bytes() {
    let mut session = Session::new();

    assert_eq!(try_sql(&mut session, "select locate('a', '中a')"), "i:2");
    assert_eq!(try_sql(&mut session, "select instr('中a', 'a')"), "i:2");

    // 6 bytes -> 48 bits.
    assert_eq!(try_sql(&mut session, "select bit_length('中a')"), "i:32");

    // The pad target is 2 CHARACTERS: one pad char fits before 中.
    assert_eq!(try_sql(&mut session, "select lpad('中', 2, 'ab')"), "s:a中");
}
