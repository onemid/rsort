#![allow(unused)]
use std::fs::{File, OpenOptions, create_dir_all};
use std::cmp::Ordering;
use std::io::{BufRead, BufReader, LineWriter, Write, Read};
use std::collections::VecDeque;
use std::str;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Queue {
    pub queue: VecDeque<RawRecord> ,
    pub current_size: usize,
    pub record_cnt: usize,
    pub end_of_record: bool
}

impl Queue {
    pub fn new_queue() -> Queue {
        Queue {
            queue: VecDeque::new(),
            current_size: 0,
            record_cnt: 0,
            end_of_record: true
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RawRecord {
    pub record_key_value: Option<String>,
    pub record_secondary_key_value: Option<String>,
    pub raw_record: String,
    pub record_size: usize,
    pub record_end: bool,
}

impl RawRecord {
    pub fn new_raw_record() -> RawRecord {
         RawRecord {
            raw_record: String::from(""),
            record_size: 0,
            record_key_value: None,
            record_secondary_key_value: None,
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

pub fn key_value(pat: &str, record: &str) -> Result<String, String> {
    let record_inner = record.clone();
    let record_inner: Vec<&str> = record_inner.split("\n").collect();
    for context in record_inner {
        if context.contains(pat) {
            return Ok(String::from(&context[pat.len()..]))
        }
    }
    Err("".to_string())
}

pub fn internal_pool_sort(internal_chunk_sort_pool: &mut Vec<RawRecord>, internal_chunk_count: usize, queue_size: usize){
    internal_chunk_sort_pool.sort_by(|a, b|
        {
            if a.record_key_value.cmp(&b.record_key_value) != Ordering::Equal {
                a.record_key_value.cmp(&b.record_key_value)
            } else {
                a.record_secondary_key_value.cmp(&b.record_secondary_key_value)
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

    let mut record_append = String::with_capacity(queue_size);
    let mut current_chunk_no = 0;
    for (idx, record) in internal_chunk_sort_pool.iter().enumerate() {
        if record_append.len() + record.raw_record.len() > queue_size {
            println!("{} {}", record_append.len(), record.raw_record.len());
            let mut chunk_file = match OpenOptions::new()
                .append(true)
                .create(true)
                .open(format!("/tmp/rec_chunk_{}/rec_{:010}", internal_chunk_count, current_chunk_no)) {
                Ok(file) => file,
                Err(error) => {
                    panic!("Something error while creating temporary record file. Details: {:?}", error);
                }
            };
            chunk_file.write(&record_append.as_bytes());
            record_append.clear();
            current_chunk_no += 1;
        } else {
            record_append.push_str(record.raw_record.as_str());
        }
    }
}

pub fn fill_the_queue(queue: &mut Queue,
                      queue_dir_num: usize,
                      queue_size: usize,
                      primary_key_pat: &String,
                      secondary_key_pat: &String) {
    // fill the queue to full
    while queue.end_of_record != true {

        let mut record_files = match File::open(
            format!("/tmp/rec_chunk_{}/{}/rec_{:010}",
                    queue_dir_num,
                    queue.record_cnt / 10000 + 1,
                    queue.record_cnt)
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

        if (queue.current_size + record_file_meta.len() as usize) < queue_size {
            let mut record = String::new();
            record_files.read_to_string(&mut record);
            // parsing primary key
            let mut primary_key_value = match key_value(
                &primary_key_pat.as_str(),
                &record.as_str()
            ) {
                Ok(str) => str,
                Err(str) => str
            };

            // parsing secondary key
            let mut secondary_key_value = match key_value(
                &secondary_key_pat.as_str(),
                &record.as_str()
            ) {
                Ok(str) => str,
                Err(str) => str
            };
            &queue.queue.push_back(RawRecord {
                raw_record: record,
                record_size: record_file_meta.len() as usize,
                record_key_value: Some(primary_key_value),
                record_secondary_key_value: Some(secondary_key_value),
                record_end: false
            });
            queue.record_cnt += 1;
            queue.current_size += record_file_meta.len() as usize;
        } else {
            return ;
        }
    }
}

pub fn winner_tree_by_idx(internal_node: &mut Vec<InternalNode>, external_node: &mut Vec<Box<Option<RawRecord>>>) -> usize {
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
                    match left_node.record_key_value.cmp(&right_node.record_key_value) {
                        Ordering::Equal => {
                            match left_node.record_secondary_key_value.cmp(&right_node.record_secondary_key_value) {
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
                    internal_node[i].ptr = Some(
                        match left_node.record_key_value.cmp(&right_node.record_key_value) {
                            Ordering::Equal => {
                                match left_node.record_secondary_key_value.cmp(&right_node.record_secondary_key_value) {
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
                        }
                    );
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

        let kv_result = match key_value("@SiteCode:", &test_str) {
            Ok(str) => str,
            Err(str) => str
        };
        assert_eq!(kv_result , "LvYHeMlIgi".to_string());

        let kv_result = match key_value("@IP:", &test_str) {
            Ok(str) => str,
            Err(str) => str
        };
        assert_eq!(kv_result , "219.85.79.132".to_string());
    }

}