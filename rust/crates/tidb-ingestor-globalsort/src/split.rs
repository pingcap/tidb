// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `split.go`: splits sorted data, by its range properties, into
//! groups of ranges (subtasks), range jobs and region split keys.

use std::collections::HashMap;
use std::sync::Arc;

use tidb_gocontext::Context;
use tidb_goruntime::go_heap;
use tidb_ingestor_simplesst::{MergePropIter, MultipleFilesStat, RangeProperty};
use tidb_objstore::storeapi::Storage;

use crate::{int_field, str_field, Result};

/// Go `writeStepMemShareCount`.
pub(crate) const WRITE_STEP_MEM_SHARE_COUNT: f64 = 6.5;

/// Go `size.SizeOfSlice` on 64-bit.
const SIZE_OF_SLICE: i64 = 24;

/// Go `exhaustedHeapElem`: a file whose last property ended at `key`.
#[derive(Clone)]
struct ExhaustedHeapElem {
    key: Vec<u8>,
    data_file: String,
    stat_file: String,
}

fn exhausted_less(a: &ExhaustedHeapElem, b: &ExhaustedHeapElem) -> bool {
    a.key < b.key
}

/// Go `CalRangeSize`: the size and keys of a range job, from the memory per
/// core and the region split size and keys.
#[must_use]
pub fn cal_range_size(mem_per_core: i64, region_split_size: i64, region_split_keys: i64) -> (i64, i64) {
    let ss = (mem_per_core as f64 / WRITE_STEP_MEM_SHARE_COUNT) as i64;
    let range_size = if ss < region_split_size {
        let range_cnt = (region_split_size as f64 / ss as f64).ceil() as i64;
        region_split_size / range_cnt + 1
    } else {
        (ss / region_split_size) * region_split_size
    };
    let avg_key_size = region_split_size as f64 / region_split_keys as f64;
    (range_size, (range_size as f64 / avg_key_size) as i64)
}

/// One group of ranges from [`RangeSplitter::split_one_ranges_group`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SplitGroup {
    /// Go `endKeyOfGroup`: empty for the last group.
    pub end_key_of_group: Vec<u8>,
    /// Go `dataFiles`.
    pub data_files: Vec<String>,
    /// Go `statFiles`.
    pub stat_files: Vec<String>,
    /// Go `interiorRangeJobKeys`.
    pub interior_range_job_keys: Vec<Vec<u8>>,
    /// Go `interiorRegionSplitKeys`.
    pub interior_region_split_keys: Vec<Vec<u8>>,
}

/// Go `RangeSplitter`: walks the merged range properties of the files and
/// cuts groups of `ranges_group_size` bytes or `ranges_group_keys` keys,
/// range jobs of `range_job_size` memory or `range_job_key_cnt` keys, and
/// region splits of `region_split_size` bytes or `region_split_key_cnt`
/// keys. A cut is taken at the first key of the next property.
pub struct RangeSplitter {
    ranges_group_size: i64,
    ranges_group_keys: i64,
    range_job_size: i64,
    range_job_key_cnt: i64,
    range_job_keys: Vec<Vec<u8>>,
    region_split_size: i64,
    region_split_key_cnt: i64,
    region_split_keys: Vec<Vec<u8>>,
    prop_iter: MergePropIter,
    multi_file_stat: Vec<MultipleFilesStat>,
    active_data_files: HashMap<String, (usize, usize)>,
    active_stat_files: HashMap<String, (usize, usize)>,
    cur_group_size: i64,
    cur_group_key_cnt: i64,
    cur_range_job_size: i64,
    cur_range_job_key_cnt: i64,
    record_range_job_after_next_prop: bool,
    cur_region_split_size: i64,
    cur_region_split_key_cnt: i64,
    record_region_split_after_next_prop: bool,
    last_data_file: String,
    last_stat_file: String,
    last_range_property: Option<RangeProperty>,
    will_exhaust_heap: Vec<ExhaustedHeapElem>,
}

impl RangeSplitter {
    /// Go `NewRangeSplitter`. Like Go, the statistics are sorted by min key
    /// before the walk.
    ///
    /// # Errors
    /// A stat file could not be opened.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: &Context,
        mut multi_file_stat: Vec<MultipleFilesStat>,
        external_storage: Arc<dyn Storage>,
        ranges_group_size: i64,
        ranges_group_key_cnt: i64,
        range_job_size: i64,
        range_job_key_cnt: i64,
        region_split_size: i64,
        region_split_key_cnt: i64,
    ) -> Result<Self> {
        tidb_log::info(
            "create range splitter",
            &[
                int_field("rangesGroupSize", ranges_group_size),
                int_field("rangesGroupKeyCnt", ranges_group_key_cnt),
                int_field("rangeJobSize", range_job_size),
                int_field("rangeJobKeyCnt", range_job_key_cnt),
                int_field("regionSplitSize", region_split_size),
                int_field("regionSplitKeyCnt", region_split_key_cnt),
            ],
        );
        let prop_iter = MergePropIter::new(ctx, &mut multi_file_stat, external_storage).map_err(crate::sst)?;
        Ok(Self {
            ranges_group_size,
            ranges_group_keys: ranges_group_key_cnt,
            range_job_size,
            range_job_key_cnt,
            range_job_keys: Vec::with_capacity(16),
            region_split_size,
            region_split_key_cnt,
            region_split_keys: Vec::with_capacity(16),
            prop_iter,
            multi_file_stat,
            active_data_files: HashMap::new(),
            active_stat_files: HashMap::new(),
            cur_group_size: 0,
            cur_group_key_cnt: 0,
            cur_range_job_size: 0,
            cur_range_job_key_cnt: 0,
            record_range_job_after_next_prop: false,
            cur_region_split_size: 0,
            cur_region_split_key_cnt: 0,
            record_region_split_after_next_prop: false,
            last_data_file: String::new(),
            last_stat_file: String::new(),
            last_range_property: None,
            will_exhaust_heap: Vec::new(),
        })
    }

    /// Go `Close`.
    ///
    /// # Errors
    /// A stat file failed to close.
    pub fn close(&mut self) -> Result<()> {
        if let Err(e) = self.prop_iter.close() {
            tidb_log::error("close range splitter error", &[str_field("error", &e.to_string())]);
            return Err(crate::sst(e));
        }
        tidb_log::info("close range splitter", &[]);
        Ok(())
    }

    /// Go `SplitOneRangesGroup`: the next group's end key (empty for the
    /// last group), the files that overlap the group, and the range job and
    /// region split keys inside it.
    ///
    /// # Errors
    /// A stat file could not be read.
    pub fn split_one_ranges_group(&mut self) -> Result<SplitGroup> {
        let (mut exhausted_data_files, mut exhausted_stat_files) = (Vec::new(), Vec::new());
        let (mut ret_data_files, mut ret_stat_files) = (Vec::new(), Vec::new());
        let mut return_after_next_prop = false;
        while self.prop_iter.next() {
            if let Some(e) = self.prop_iter.error() {
                return Err(crate::sst(e.clone()));
            }
            let prop = self.prop_iter.curr_property().clone();
            let (size, keys) = (prop.size as i64, prop.keys as i64);
            self.cur_group_size += size;
            self.cur_range_job_size += size;
            self.cur_region_split_size += size;
            self.cur_group_key_cnt += keys;
            self.cur_range_job_key_cnt += keys;
            self.cur_region_split_key_cnt += keys;

            // The last property was the last of its file.
            if self.prop_iter.get_base_iter_close_reader_flag() {
                let last = self.last_range_property.as_ref().expect("a property before a closed reader");
                go_heap_push(
                    &mut self.will_exhaust_heap,
                    ExhaustedHeapElem {
                        key: last.last_key.clone(),
                        data_file: self.last_data_file.clone(),
                        stat_file: self.last_stat_file.clone(),
                    },
                );
            }

            let (idx, idx2) = self.prop_iter.reader_index();
            let file_pair = &self.multi_file_stat[idx].filenames[idx2];
            let (data_file_path, stat_file_path) = (file_pair[0].clone(), file_pair[1].clone());
            self.active_data_files.insert(data_file_path.clone(), (idx, idx2));
            self.active_stat_files.insert(stat_file_path.clone(), (idx, idx2));
            self.last_data_file = data_file_path;
            self.last_stat_file = stat_file_path;
            self.last_range_property = Some(prop.clone());

            while !self.will_exhaust_heap.is_empty() && self.will_exhaust_heap[0].key < prop.first_key {
                let e = go_heap::pop(&mut self.will_exhaust_heap, &mut exhausted_less).expect("non-empty heap");
                exhausted_data_files.push(e.data_file);
                exhausted_stat_files.push(e.stat_file);
            }

            if return_after_next_prop {
                for p in exhausted_data_files.drain(..) {
                    self.active_data_files.remove(&p);
                }
                for p in exhausted_stat_files.drain(..) {
                    self.active_stat_files.remove(&p);
                }
                return Ok(SplitGroup {
                    end_key_of_group: prop.first_key,
                    data_files: ret_data_files,
                    stat_files: ret_stat_files,
                    interior_range_job_keys: std::mem::take(&mut self.range_job_keys),
                    interior_region_split_keys: std::mem::take(&mut self.region_split_keys),
                });
            }
            if self.record_range_job_after_next_prop {
                self.range_job_keys.push(prop.first_key.clone());
                self.record_range_job_after_next_prop = false;
            }
            if self.record_region_split_after_next_prop {
                self.region_split_keys.push(prop.first_key.clone());
                self.record_region_split_after_next_prop = false;
            }

            let range_mem_size = self.cur_range_job_size + self.cur_range_job_key_cnt * SIZE_OF_SLICE * 2;
            if range_mem_size >= self.range_job_size || self.cur_range_job_key_cnt >= self.range_job_key_cnt {
                self.cur_range_job_size = 0;
                self.cur_range_job_key_cnt = 0;
                self.record_range_job_after_next_prop = true;
            }
            if self.cur_region_split_size >= self.region_split_size
                || self.cur_region_split_key_cnt >= self.region_split_key_cnt
            {
                self.cur_region_split_size = 0;
                self.cur_region_split_key_cnt = 0;
                self.record_region_split_after_next_prop = true;
            }
            if self.cur_group_size >= self.ranges_group_size || self.cur_group_key_cnt >= self.ranges_group_keys {
                (ret_data_files, ret_stat_files) = self.clone_active_files();
                self.cur_group_size = 0;
                self.cur_group_key_cnt = 0;
                return_after_next_prop = true;
            }
        }
        let (data_files, stat_files) = self.clone_active_files();
        self.active_data_files.clear();
        self.active_stat_files.clear();
        if let Some(e) = self.prop_iter.error() {
            return Err(crate::sst(e.clone()));
        }
        Ok(SplitGroup {
            end_key_of_group: Vec::new(),
            data_files,
            stat_files,
            interior_range_job_keys: std::mem::take(&mut self.range_job_keys),
            interior_region_split_keys: std::mem::take(&mut self.region_split_keys),
        })
    }

    /// Go `cloneActiveFiles`: in statistic and file order.
    fn clone_active_files(&self) -> (Vec<String>, Vec<String>) {
        let sorted = |m: &HashMap<String, (usize, usize)>| {
            let mut v: Vec<(&String, &(usize, usize))> = m.iter().collect();
            v.sort_by_key(|(_, idx)| **idx);
            v.into_iter().map(|(p, _)| p.clone()).collect::<Vec<_>>()
        };
        (sorted(&self.active_data_files), sorted(&self.active_stat_files))
    }
}

/// Go `heap.Push`.
fn go_heap_push(h: &mut Vec<ExhaustedHeapElem>, x: ExhaustedHeapElem) {
    h.push(x);
    let n = h.len() - 1;
    go_heap::up(h, n, &mut exhausted_less);
}
