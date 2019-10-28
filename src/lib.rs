#![allow(unused)]
extern crate rayon;
use std::fs::{File, OpenOptions, create_dir_all};
use std::cmp::Ordering;
use std::io::{BufRead, BufReader, LineWriter, BufWriter, Write, Read};
use std::collections::VecDeque;
use std::str;
use std::slice;
use std::mem;
use std::env::current_dir;
use rayon::prelude::*;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Queue {
    pub queue: VecDeque<RawRecord> ,
    pub current_chunk: usize,
    pub end_of_record: bool
}

impl Queue {
    pub fn new_queue() -> Queue {
        Queue {
            queue: VecDeque::new(),
            current_chunk: 0,
            end_of_record: true
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RawRecord {
    pub raw_record: String,
    pub key_pos: Vec<(usize, usize)>, // key start, key length; if no key, then kl is 0
    pub record_end: bool,
}

impl RawRecord {
    pub fn new_raw_record() -> RawRecord {
         RawRecord {
            raw_record: String::from(""),
            key_pos: vec![(0, 0)],
            record_end: true,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InternalNode {
    pub is_leaf: bool,
    pub ptr: Option<usize> // None represent the node hasn't init.
}

impl InternalNode {
    pub fn new_leaf_inode() -> InternalNode {
        InternalNode {
            is_leaf: true,
            ptr: None
        }
    }

    pub fn new_non_leaf_inode() -> InternalNode {
        InternalNode {
            is_leaf: false,
            ptr: None
        }
    }
}

pub fn key_value<'a>(pat: &str, record: &'a str) -> Vec<&'a str> {
    let record_inner: Vec<&str> = record.split("\n").filter(|&context| {
        context.starts_with(pat)
    }).collect();
    record_inner
}

pub fn key_pos(pat: &str, record: &str) -> (usize, usize) {
    let mut pos = (0, 0);
    let find_result = record.find(pat);
    if find_result.is_some() {
        pos.0 = find_result.unwrap();
        pos.1 = match record[pos.0..].find('\n') {
            Some(key_start) => pos.0 + key_start,
            None => pos.0 + record.len()
        };
    }
    pos
}
pub fn internal_pool_sort(internal_chunk_sort_pool: &mut Vec<RawRecord>,
                          internal_chunk_count: usize,
                          queue_size: usize,
                          primary_key_pat: &str,
                          secondary_key_pat: &str){
    println!("Into Internal Pool Sort");
    internal_chunk_sort_pool.par_sort_unstable_by( |a, b|
        {
            let a_record_key_value = &a.raw_record.as_str()[a.key_pos[0].0..a.key_pos[0].1];
            let b_record_key_value = &b.raw_record.as_str()[b.key_pos[0].0..b.key_pos[0].1];
            let cmp_result = a_record_key_value.cmp(&b_record_key_value);
            if cmp_result != Ordering::Equal {
                cmp_result
            } else {
                let a_record_secondary_key_value = &a.raw_record.as_str()[a.key_pos[1].0..a.key_pos[1].1];
                let b_record_secondary_key_value = &b.raw_record.as_str()[b.key_pos[1].0..b.key_pos[1].1];
                a_record_secondary_key_value.cmp(&b_record_secondary_key_value)
            }
        }); // ASC default
    println!("/tmp/rec_chunk_{:}", internal_chunk_count);

    match create_dir_all(format!("/tmp/rec_chunk_{:}", internal_chunk_count)) {
        Ok(file) => (),
        Err(error) => {
            panic!("Something error while creating temporary record directory. Details: {:?}", error);
        }
    };

    let mut dir_set = 0;

    let mut record_map: Vec<usize> = Vec::new();
    record_map.push(0);
    let mut record_key_map: Vec<usize> = Vec::new();
    let mut current_chunk_no = 0;
    let mut current_size = 0;

    let mut chunk_file = BufWriter::new(match OpenOptions::new()
        .append(true)
        .create(true)
        .open(format!("/tmp/rec_chunk_{}/rec_{:010}", internal_chunk_count, current_chunk_no)) {
        Ok(file) => file,
        Err(error) => {
            panic!("Something error while creating temporary record file. Details: {:?}", error);
        }
    });
    let mut chunk_file_map = match OpenOptions::new()
        .append(true)
        .create(true)
        .open(format!("/tmp/rec_chunk_{}/rec_map_{:010}", internal_chunk_count, current_chunk_no)) {
        Ok(file) => file,
        Err(error) => {
            panic!("Something error while creating temporary record file. Details: {:?}", error);
        }
    };
    let mut chunk_file_key_map = match OpenOptions::new()
        .append(true)
        .create(true)
        .open(format!("/tmp/rec_chunk_{}/rec_key_map_{:010}", internal_chunk_count, current_chunk_no)) {
        Ok(file) => file,
        Err(error) => {
            panic!("Something error while creating temporary record file. Details: {:?}", error);
        }
    };

    for (idx, record) in internal_chunk_sort_pool.iter().enumerate() {
        if current_size + record.raw_record.len() > queue_size {
            let slice_u8 = unsafe {
                slice::from_raw_parts(
                    record_map.as_ptr() as *const u8,
                    record_map.len() * mem::size_of::<usize>(),
                )
            };
            let key_map_u8 = unsafe {
                slice::from_raw_parts(
                    record_key_map.as_ptr() as *const u8,
                    record_key_map.len() * mem::size_of::<usize>(),
                )
            };
            chunk_file_map.write_all(&slice_u8);
            chunk_file_key_map.write_all(&key_map_u8);
            record_map.clear();
            record_key_map.clear();
            current_size = 0;
            record_map.push(0);

            current_chunk_no += 1;
            chunk_file = BufWriter::new(match OpenOptions::new().append(true).create(true)
                .open(format!("/tmp/rec_chunk_{}/rec_{:010}", internal_chunk_count, current_chunk_no)) {
                Ok(file) => file,
                Err(error) => {
                    panic!("Something error while creating temporary record file. Details: {:?}", error);
                }
            });
            chunk_file_map = match OpenOptions::new().append(true).create(true)
                .open(format!("/tmp/rec_chunk_{}/rec_map_{:010}", internal_chunk_count, current_chunk_no)) {
                Ok(file) => file,
                Err(error) => {
                    panic!("Something error while creating temporary record file. Details: {:?}", error);
                }
            };
            chunk_file_key_map = match OpenOptions::new().append(true).create(true)
                .open(format!("/tmp/rec_chunk_{}/rec_key_map_{:010}", internal_chunk_count, current_chunk_no)) {
                Ok(file) => file,
                Err(error) => {
                    panic!("Something error while creating temporary record file. Details: {:?}", error);
                }
            };

        } else {
            chunk_file.write(&record.raw_record.as_bytes());
            current_size += record.raw_record.len();
            record_map.push(current_size);
            record_key_map.push(record.key_pos[0].0);
            record_key_map.push(record.key_pos[0].1);
            record_key_map.push(record.key_pos[1].0);
            record_key_map.push(record.key_pos[1].1);
        }
    }
    let slice_u8 = unsafe {
        slice::from_raw_parts(
            record_map.as_ptr() as *const u8,
            record_map.len() * mem::size_of::<usize>(),
        )
    };
    let key_map_u8 = unsafe {
        slice::from_raw_parts(
            record_key_map.as_ptr() as *const u8,
            record_key_map.len() * mem::size_of::<usize>(),
        )
    };
    chunk_file_map.write_all(&slice_u8);
    chunk_file_key_map.write_all(&key_map_u8);
}

pub fn fill_the_queue(queue: &mut Queue,
                      chunk_dir_serial: usize,
                      queue_size: usize,
                      primary_key_pat: &String,
                      secondary_key_pat: &String) {
    // fill the queue to full
    while queue.end_of_record != true && queue.queue.len() == 0 {

        let mut record_files = match File::open(
            format!("/tmp/rec_chunk_{}/rec_{:010}",
                    chunk_dir_serial,
                    queue.current_chunk)
        ) {
            Ok(rec_file) => rec_file,
            Err(error) => {
                queue.end_of_record = true;
                return;
            }
        };

        let record_file_meta = match record_files.metadata() {
            Ok(file_meta) => file_meta,
            Err(error) => {
                panic!("Cannot read the metadata from the record file. Details: {:?}", error);
            }
        };

        let mut record_map_files = match File::open(
            format!("/tmp/rec_chunk_{}/rec_map_{:010}",
                    chunk_dir_serial,
                    queue.current_chunk)
        ) {
            Ok(rec_file) => rec_file,
            Err(error) => {
                queue.end_of_record = true;
                return;
            }
        };

        let mut record_key_map_files = match File::open(
            format!("/tmp/rec_chunk_{}/rec_key_map_{:010}",
                    chunk_dir_serial,
                    queue.current_chunk)
        ) {
            Ok(rec_file) => rec_file,
            Err(error) => {
                queue.end_of_record = true;
                return;
            }
        };

        let mut record_all = String::new();
        record_files.read_to_string(&mut record_all);

        let mut map_u8 = Vec::new();
        record_map_files.read_to_end(&mut map_u8);

        let mut key_map_u8 = Vec::new();
        record_key_map_files.read_to_end(&mut key_map_u8);

        let mut map_usize = unsafe {
          slice::from_raw_parts(map_u8.as_ptr() as *const usize,
          map_u8.len() * mem::size_of::<u8>())
        };

        let mut key_map_usize = unsafe {
            slice::from_raw_parts(key_map_u8.as_ptr() as *const usize,
                                  key_map_u8.len() * mem::size_of::<u8>())
        };

        for i in 0..map_usize.len()-1 {
            if map_usize[i] > map_usize[i+1] || map_usize[i] > record_all.len() || map_usize[i+1] > record_all.len() {break;}
            let mut record = String::from(&record_all[map_usize[i]..map_usize[i+1]]);
            let mut key_pos = vec![(key_map_usize[i*4], key_map_usize[i*4+1]), (key_map_usize[i*4+2], key_map_usize[i*4+3])];
            &queue.queue.push_back(RawRecord {
                raw_record: record,
                key_pos,
                record_end: false
            });
        }
        queue.current_chunk += 1;
    }
}

pub fn winner_tree_by_idx(internal_node: &mut Vec<InternalNode>, external_node: &mut Vec<Box<Option<RawRecord>>>, primary_key_pat: &str, secondary_key_pat: &str) -> usize {
    // initialising the internal node leaf by looking up the external node
    let mut i_tree_size = internal_node.len(); // internal tree size
    let mut terminator_pos = i_tree_size;
    let mut e_cur_cnt = 0;
    for i in i_tree_size/2..i_tree_size {
        if internal_node[i].is_leaf == true && internal_node[i].ptr == None {
            let left_node = match &*external_node[e_cur_cnt] {
                Some(rec) => { rec.clone() },
                None => {RawRecord::new_raw_record()}
            };
            let right_node = match &*external_node[e_cur_cnt + 1] {
                Some(rec) => { rec.clone() },
                None => {RawRecord::new_raw_record()}
            };

            if left_node.record_end == true && right_node.record_end == true {
                internal_node[i].ptr = Some(terminator_pos);
            } else if left_node.record_end == true && right_node.record_end == false {
                internal_node[i].ptr = Some(e_cur_cnt + 1);
            } else if left_node.record_end == false && right_node.record_end == true{
                internal_node[i].ptr = Some(e_cur_cnt);
            } else {
                internal_node[i].ptr = Some(
                    match left_node.raw_record[left_node.key_pos[0].0..left_node.key_pos[0].1]
                        .cmp(&right_node.raw_record[right_node.key_pos[0].0..right_node.key_pos[0].1]) {
                        Ordering::Equal => {
                            match left_node.raw_record[left_node.key_pos[1].0..left_node.key_pos[1].1]
                                .cmp(&right_node.raw_record[right_node.key_pos[1].0..right_node.key_pos[1].1]) {
                                Ordering::Equal => { e_cur_cnt }, // left node
                                Ordering::Greater => { e_cur_cnt + 1 }, // right node
                                Ordering::Less => { e_cur_cnt } // left node
                            }
                        },
                        Ordering::Greater => {
                            e_cur_cnt + 1
                        }, // right node
                        Ordering::Less => {
                            e_cur_cnt
                        } // left node
                    });
            }
        }
        e_cur_cnt += 2;
    }

    i_tree_size /= 2;

    // performing the sibling comparision in the internal non-leaf node
    // We promise the leaf does not contain the None.
    while i_tree_size > 1 {
        for i in i_tree_size/2..i_tree_size {
            let left_node_idx = match internal_node[i*2].ptr {
                Some(idx) =>  { idx },
                None => { panic!("The leaf node contains the None initialised value.") }
            };
            let right_node_idx = match internal_node[i*2+1].ptr {
                Some(idx) =>  { idx },
                None => { panic!("The leaf node contains the None initialised value.") }
            };

            if internal_node[i].is_leaf == false && internal_node[i].ptr == None {
                let left_node = match &*external_node[left_node_idx] {
                    Some(rec) => { rec.clone() },
                    None => {RawRecord::new_raw_record()}
                };
                let right_node = match &*external_node[right_node_idx] {
                    Some(rec) => { rec.clone() },
                    None => {RawRecord::new_raw_record()}
                };

                if left_node.record_end == true && right_node.record_end == true {
                    internal_node[i].ptr = Some(terminator_pos);
                } else if left_node.record_end == true && right_node.record_end == false {
                    internal_node[i].ptr = Some(right_node_idx);
                } else if left_node.record_end == false && right_node.record_end == true{
                    internal_node[i].ptr = Some(left_node_idx);
                } else {
                    internal_node[i].ptr = Some(match left_node.raw_record[left_node.key_pos[0].0..left_node.key_pos[0].1].cmp(&right_node.raw_record[right_node.key_pos[0].0..right_node.key_pos[0].1]) {
                        Ordering::Equal => {
                            match left_node.raw_record[left_node.key_pos[1].0..left_node.key_pos[1].1].cmp(&right_node.raw_record[right_node.key_pos[1].0..right_node.key_pos[1].1]) {
                                Ordering::Equal => { left_node_idx }, // left node
                                Ordering::Greater => { right_node_idx }, // right node
                                Ordering::Less => { left_node_idx } // left node
                            }
                        },
                        Ordering::Greater => {
                            right_node_idx
                        }, // right node
                        Ordering::Less => {
                            left_node_idx
                        } // left node
                    });
                }
            }
        }
        i_tree_size /= 2;
    }

    let max_ptr = match internal_node[1].ptr {
        Some(idx) => { idx },
        None => { panic!("Should not be None, if empty, each one should be inf rec."); }
    };

    // popping up the value and reset the winner node to None
    for i in 0..internal_node.len() {
        if internal_node[i].ptr == Some(max_ptr) {
            internal_node[i].ptr = None;
        }
    }

    max_ptr
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_value_fetching() {
        let test_str = "\
        @
@Gais_REC:
@url:http://travel.ettoday.net/article/718757.htm
@MainTextMD5:892FF0D0A82D5CA1DB70A320611A9BE9
@UntagMD5:E9D666D79C1A4D3337720C920F2E5A5F
@SiteCode:LvYHeMlIgi
@UrlCode:ACC79899BE4BEBDDC236B50C3979023A
@title:修杰楷訓練愛女自己吃飯　咘咘完食「拍手鼓掌」萌翻！ | ETtoday 東森旅遊雲 | ETtoday旅遊新聞(影劇)
@Size:89230
@keyword:東森,記者,藝人,賈靜雯,育兒生活
@image_links:http://static.ettoday.net/images/1853/d1853989.jpg
@Fetchtime:2017/01/10 23:15:09
@post_time:2016/06/17 00:00:00
@Ref:http://travel.ettoday.net/article/718839.htm
@BodyMD5:818B0989AB17758DA4E17604D1B47052
@Lang:utf-8
@IP:219.85.79.132
@body: 修杰楷訓練愛女自己吃飯　咘咘完食「拍手鼓掌」萌翻！ | ETtoday 東森旅遊雲 | ETtoday旅遊新聞(影劇)
2016年06月17日 22:18
記者黃庠棻／綜合報導 藝人修杰楷出道13年，2015年5月和大9歲的賈靜雯結婚，同年生下一女咘咘，夫妻倆常常會在臉書分享育兒生活，每次都會吸引大批網友迴響，前不久才在新北市政府服替代役的他近日放假，回到家中陪伴女兒，17日晚間又貼出一段訓練咘咘自己吃飯的影片，可愛的模樣造成粉絲熱烈討論。 ▲賈靜雯和修杰楷常會在臉書分享育兒生活。（圖／翻攝自修杰楷臉書） 修杰楷17日貼出一段咘咘吃飯的影片，表示自己開啟了課，要訓練女兒「吃東西就是要自己來」，只見咘咘坐在嬰兒用座椅，靠著自己的力量，抓著碗裡的食物往嘴塞，雖然動作還有些生澀、笨拙，但不用爸媽餵食，成功吃到東西的模樣也讓許多網友感到相當感動，紛紛大讚「咘咘會自己吃飯啦！」 ▲修杰楷貼出訓練咘咘自己吃飯的影片。（圖／翻攝自修杰楷臉書） 不僅如此，咘咘在連續兩次成功靠自身力量吃到飯之後，竟然伸出肉嘟嘟的雙手「拍手鼓掌」，就像自我鼓勵一樣，逗趣的舉動讓大批粉絲不僅笑成一片，也紛紛直呼「要被萌翻了啦！」該則影片也憑著她的高人氣，才貼出短短1小時就吸引超過4萬個人按讚。 ▲咘咘成功吃完飯後，竟然自己拍手鼓勵，可愛的模樣引起網友討論。（圖／翻攝自修杰楷臉書） ";

        let kv_result = key_value("@SiteCode:", &test_str);
        assert_eq!(kv_result[0] , "LvYHeMlIgi".to_string());

        let kv_result = key_value("@IP:", &test_str);
        assert_eq!(kv_result[0] , "219.85.79.132".to_string());
    }

}