// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use super::{ScalarFunction, vec_eval};
use crate::builtin_ext::{compare2, misc};
use crate::{Columns, EvalError};
use std::fmt::Write;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::EvalType;
use tidb_proto::tipb::ScalarFuncSig;

pub(super) fn eval(
    sig: ScalarFuncSig,
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Option<Result<(), EvalError>> {
    use ScalarFuncSig::*;
    Some(match sig {
        IsIPv4 => predicate(function, ctx, input, result, compare2::is_ipv4_bytes),
        IsIPv6 => predicate(function, ctx, input, result, |bytes| {
            std::str::from_utf8(bytes).is_ok_and(|text| text.parse::<Ipv6Addr>().is_ok())
        }),
        IsIPv4Compat => predicate(function, ctx, input, result, compare2::is_ipv4_compat_bytes),
        IsIPv4Mapped => predicate(function, ctx, input, result, compare2::is_ipv4_mapped_bytes),
        IsUuid => predicate(function, ctx, input, result, misc::is_uuid_bytes),
        Inet6Aton => parse_address(function, ctx, input, result),
        InetNtoa => format_ipv4(function, ctx, input, result),
        Inet6Ntoa => format_address(function, ctx, input, result),
        _ => return None,
    })
}

fn predicate(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
    test: impl Fn(&[u8]) -> bool,
) -> Result<(), EvalError> {
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::String,
        ctx,
        input,
        |column, selection| {
            vec_eval::map_variable_int64(column, selection, result, |bytes| i64::from(test(bytes)));
        },
    )
}

fn parse_address(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::String,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let get = cells.variable_cells();
            let rows = input.num_rows();
            result.reserve_string_with_size_hint(rows, 16);
            let mut ipv6 = [0; 16];
            result.try_append_byte_rows(rows, |row, output| {
                let row = selection.map_or(row, |select| select[row]);
                if cells.is_null(row) {
                    return Ok(false);
                }
                match std::str::from_utf8(get(row))
                    .ok()
                    .and_then(|text| text.parse().ok())
                {
                    Some(IpAddr::V4(address)) => output.append(&address.octets()),
                    Some(IpAddr::V6(address)) => {
                        let bytes = address.octets();
                        if compare2::is_ipv4_mapped_bytes(&bytes) {
                            // Go's vector kernel only overwrites the last
                            // six bytes of its shared IPv6 buffer here.
                            ipv6[10..].copy_from_slice(&bytes[10..]);
                        } else {
                            ipv6 = bytes;
                        }
                        output.append(&ipv6);
                    }
                    None => return Ok(false),
                }
                Ok(true)
            })
        },
    )?
}

fn format_ipv4(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::Int,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let input_values = cells.fixed_cells::<8>();
            let rows = input.num_rows();
            let mut scratch = [0; 15];
            result.reserve_string(rows);
            result.try_append_byte_rows(rows, |row, output| {
                let row = selection.map_or(row, |select| select[row]);
                if cells.is_null(row) {
                    return Ok(false);
                }
                let Ok(address) = u32::try_from(u64::from_ne_bytes(input_values[row])) else {
                    return Ok(false);
                };
                output.append(render_ipv4(address.to_be_bytes(), &mut scratch));
                Ok(true)
            })
        },
    )?
}

const IPV4_DECIMAL: [[u8; 4]; 256] = {
    let mut table = [[0; 4]; 256];
    let mut i = 0;
    while i < table.len() {
        let value = i as u8;
        table[i] = if value < 10 {
            [b'0' + value, 0, 0, 1]
        } else if value < 100 {
            [b'0' + value / 10, b'0' + value % 10, 0, 2]
        } else {
            [
                b'0' + value / 100,
                b'0' + value / 10 % 10,
                b'0' + value % 10,
                3,
            ]
        };
        i += 1;
    }
    table
};

fn render_ipv4(address: [u8; 4], target: &mut [u8; 15]) -> &[u8] {
    let mut len = 0;
    for (i, octet) in address.into_iter().enumerate() {
        if i != 0 {
            target[len] = b'.';
            len += 1;
        }
        let digits = &IPV4_DECIMAL[usize::from(octet)];
        target[len..len + 3].copy_from_slice(&digits[..3]);
        len += usize::from(digits[3]);
    }
    &target[..len]
}

fn format_address(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::String,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let get = cells.variable_cells();
            let rows = input.num_rows();
            let mut scratch = String::with_capacity(39);
            result.reserve_string_with_size_hint(rows, 24);
            result.try_append_byte_rows(rows, |row, output| {
                let row = selection.map_or(row, |select| select[row]);
                if cells.is_null(row) {
                    return Ok(false);
                }
                scratch.clear();
                let bytes = get(row);
                if let Ok(bytes) = <[u8; 4]>::try_from(bytes) {
                    write!(&mut scratch, "{}", Ipv4Addr::from(bytes)).expect("writing a string");
                } else if let Ok(bytes) = <[u8; 16]>::try_from(bytes) {
                    write!(&mut scratch, "{}", Ipv6Addr::from(bytes)).expect("writing a string");
                } else {
                    return Ok(false);
                }
                output.append(scratch.as_bytes());
                Ok(true)
            })
        },
    )?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NoColumns;
    use crate::evaluator::EvaluatorSuite;
    use crate::expression::Expression;
    use crate::new_function::new_function_base;
    use tidb_datatype::{FieldType, FieldTypeCode};

    #[test]
    fn inet6_aton_retains_go_buffer_prefix_in_selected_order() {
        let field = FieldType::new(FieldTypeCode::VarString);
        let mut source = Chunk::new_chunk_with_capacity(std::slice::from_ref(&field), 4);
        source.append_bytes(0, b"::ffff:192.0.2.1");
        source.append_bytes(0, b"2001:db8::1");
        source.append_bytes(0, b"192.0.2.2");
        source.append_null(0);
        source.set_sel(Some(vec![0, 1, 2, 3, 0]));
        let mut argument = crate::column::Column::new(1, field.clone());
        argument.index = 0;
        let function = new_function_base(
            &NoColumns,
            "inet6_aton",
            field.clone(),
            vec![Expression::Column(argument)],
        )
        .unwrap();
        let suite = EvaluatorSuite::new(vec![function], false);
        let mut output = Chunk::new_chunk_with_capacity(&[field], 5);
        suite.run(&NoColumns, &mut source, &mut output).unwrap();
        let values = output.column(0);
        assert_eq!(
            values.get_raw(0),
            &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 192, 0, 2, 1]
        );
        assert_eq!(values.get_raw(2), &[192, 0, 2, 2]);
        assert!(values.is_null(3));
        assert_eq!(
            values.get_raw(4),
            &[32, 1, 13, 184, 0, 0, 0, 0, 0, 0, 255, 255, 192, 0, 2, 1]
        );
    }
}
