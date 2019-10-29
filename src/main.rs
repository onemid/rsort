use std::fs::{File, OpenOptions, remove_dir_all};
use std::io::{BufRead, BufReader, Write, BufWriter};
use std::collections::VecDeque;
use rsort::{key_value, fill_the_queue, winner_tree_by_idx, internal_pool_sort, InternalNode, RawRecord, Queue, key_pos};
use std::cmp::Ordering;
use std::time::{Duration, Instant};

fn main() {
    //find . -name 'rec_*' | xargs rm
    let filename = String::from("youtube2017.0000.rec");
    let rec_begin_pat = String::from("@\n");
    let primary_key_pat = String::from("@viewCount:");
    let secondary_key_pat = String::from("@duration:");

//    let filename = String::from("ettoday.rec");
//    let rec_begin_pat = String::from("@Gais_REC:\n");
//    let primary_key_pat = String::from("@url:");
//    let secondary_key_pat = String::from("@url:");


    let file = match File::open(filename) {
        Ok(file) => file,
        Err(error) => {
            panic!("Something when wrong while opening the file. Details: {:?}", error);
        }
    };
    let file_meta = match file.metadata() {
        Ok(file_meta) => file_meta,
        Err(error) => {
            panic!("Cannot read the metadata from the file. Details: {:?}", error);
        }
    };

    // The initial settings
    // ---------------------M------K------B---
    let memory_size: usize = 512 * 1024 * 1024; // 16 GB
    let chunk_size: usize = memory_size / 2;
    let total_size: usize = file_meta.len() as usize; // this is the total file size
    let queue_count: usize = match total_size / chunk_size == 0 {
        true => 1,
        false => 2.0f64.powf(((total_size as f64 / chunk_size as f64).log2()).ceil()) as usize
    };
    // queue_count, or called K-way
    // the chuck_size must be the power of 2; the formula is 2 ^ ceil of lg N.

    let queue_size: usize =  (chunk_size as f64 / queue_count as f64).ceil() as usize;
    // there are 2-way to pick up the queue_size, one is mem_size/queue_count,
    // but if the total data cannot distribute evenly, we may calc the total rec size and div by queue_count

    let mut internal_chunk_sort_pool: Vec<RawRecord> = Vec::with_capacity(chunk_size);
    let mut internal_chunk_sort_pool_cur_size = 0;
    let mut internal_chunk_count = 0;

    println!("{} {} {} {}",  chunk_size, total_size, queue_count, queue_size);

    // To parsing the record, using BufReader
    let mut reader = BufReader::new(file);
    let mut line: Vec<u8> = Vec::new();
    let mut record_tmp: String = String::new();

    let start = Instant::now();
    if true {
        while match reader.read_until(b'\n', &mut line) {
            Ok(read_size) => read_size > 0,
            Err(error) => {
                panic!("Something error while reading line. Details: {:?}", error);
            }
        } {
            let repaired_line = String::from_utf8_lossy(&line);

            if repaired_line.starts_with(&rec_begin_pat) {
                // write back the record
                // 1. check the record_tmp len
                if record_tmp.len() > 0 {
                    if internal_chunk_sort_pool_cur_size + record_tmp.len() < chunk_size {
                        let mut key_pos =
                            vec![key_pos(&primary_key_pat, &record_tmp),
                                 key_pos(&secondary_key_pat, &record_tmp)];
                        internal_chunk_sort_pool.push(RawRecord {
                            raw_record: record_tmp.clone(),
                            key_pos,
                            record_end: false
                        });
                        internal_chunk_sort_pool_cur_size += record_tmp.len()
                    } else { // performing internal sort and write back to the file
                        internal_pool_sort(&mut internal_chunk_sort_pool, internal_chunk_count, queue_size, &primary_key_pat, &secondary_key_pat);
                        internal_chunk_sort_pool.clear();
                        internal_chunk_sort_pool_cur_size = 0;
                        internal_chunk_count += 1;
                    }

                    record_tmp.clear();

                }
            }
            record_tmp.push_str(&repaired_line);
            line.clear();
        }
        // write back the remain things
        internal_pool_sort(&mut internal_chunk_sort_pool, internal_chunk_count, queue_size, &primary_key_pat, &secondary_key_pat);
        internal_chunk_sort_pool.clear();
    }

    let duration = start.elapsed();
    println!("Time elapsed in Initialising(Data Preprocess) is: {:?}", duration);
    // Performing the K-way external merge sort
    // initialising the K-way buffer
    // we confirm that all the queues have the data
    // ready to do K-way external merge-sort
    // Strategies -- the loop:
    // 1. pick up the record from top of queues
    // P.S. because loser(winner) tree is completed binary tree; thus, we might impl by array
    // 2. pick up the min/max which was generated by the tournament tree.
    // 3. check each queue whether has been already empty.


    let record_queue = Queue {
        queue: VecDeque::with_capacity(queue_size),
        current_chunk: 0,
        end_of_record: false
    };
//    let mut queue_pool: Arc<Vec<Box<Queue>>> = Arc::new(vec![Box::new(record_queue); queue_count]);
    let mut queue_pool: Vec<Box<Queue>> = vec![Box::new(record_queue); queue_count];

    // Compute the total loser tree elements
    let i_ele_size = queue_count;
    let e_ele_size = queue_count;

    // Initialising the winner tree
    let mut external_node: Vec<Box<Option<RawRecord>>> =vec![Box::new(None); e_ele_size];
    external_node.push(Box::new(Some(RawRecord::new_raw_record()))); // set a terminator

    let mut internal_node = vec![InternalNode::new_non_leaf_inode(); i_ele_size / 2];
    for _i in 0..i_ele_size / 2 {
        internal_node.push(InternalNode::new_leaf_inode())
    }

    let mut rec_cnt = 0;


    println!("Starting to sort");
    let mut result_file = BufWriter::new(match OpenOptions::new()
        .write(true)
        .create(true)
        .open(format!("/tmp/result_rec_url")) {
        Ok(file) => file,
        Err(error) => {
            panic!("Something error while creating temporary result record file. Details: {:?}", error);
        }
    });

    let mut previous_record = RawRecord::new_raw_record();
    let start = Instant::now();
    loop {

        // Iterating all the first element in each queue, and load the record from the file

        for i in 0..queue_count {
            // check the queue top whether is the empty mark
            let mut queue = &mut queue_pool[i];
            // Initialising the queue.
            fill_the_queue(&mut queue, i, queue_size, &primary_key_pat, &secondary_key_pat);
        }



        // 1. Pick up the record from top of queues, the initial run.
        for i in 0..queue_count {
            if *external_node[i] == None {
                let mut queue = &mut queue_pool[i];
                let rec = queue.queue.pop_front();
                *external_node[i] = rec;
            }
        }

        // 2. Send the winner tree array to loser tree function to choose the winner
        let top = winner_tree_by_idx(&mut internal_node, &mut external_node, &primary_key_pat, &secondary_key_pat);
//        rec_cnt += 1;
//        if rec_cnt % 10000 == 0 {
//            println!("{}", rec_cnt);
//        }

        match &*external_node[top] {
            Some(rec) => {
//                let r = match &rec.record_key_value {
//                    Some(s) => s.clone(),
//                    None => "".to_string()
//                };
                // verify the monotonic inc.
//                if rec_cnt > 1 {
//                    match previous_record.raw_record[previous_record.key_pos[0].0..previous_record.key_pos[0].1]
//                        .cmp(&rec.raw_record[rec.key_pos[0].0..rec.key_pos[0].1]) {
//                        Ordering::Equal => {
//                            match previous_record.raw_record[previous_record.key_pos[1].0..previous_record.key_pos[1].1]
//                                .cmp(&rec.raw_record[rec.key_pos[1].0..rec.key_pos[1].1]) {
//                                Ordering::Greater => { println!("SECONDARY_KEY ERROR, should be smaller {}", rec_cnt); }, // right node
//                                Ordering::Less => {}, // left node
//                                _ => {}
//                            }
//                        },
//                        Ordering::Greater => {
//                            println!("PRIMARY_KEY: ERROR, should be smaller {}", rec_cnt);
//                        }, // right node
//                        Ordering::Less => {} // left node
//                    }
//                }
//
//                previous_record = rec.clone();
                match result_file.write(rec.raw_record.as_bytes()) {
                    Ok(_size) => (),
                    Err(_e) => {panic!("Write error");}
                }
//                match result_file.write('\n'.to_string().as_bytes()) {
//                    Ok(_size) => (),
//                    Err(_e) => {panic!("Write error");}
//                }
            },
            None => {
                break;
            }
        }

        if *external_node[top] == None {
            for i in 0..queue_count {
                // check the queue top whether is the empty mark
                let queue = &mut queue_pool[i];
                if queue.end_of_record == false {
                    println!("INT{:?}", internal_node);
                    println!("EXN{:?}", external_node);
                    panic!("The queue should be empty");
                }
            }
            break;
        }

        *external_node[top] = None;

    }
    let duration = start.elapsed();
    println!("Time elapsed in K-way external sorting is: {:?}", duration);

//     clean up the file
    for i in 0..queue_count {
        match remove_dir_all(format!("/tmp/rec_chunk_{}", i)) {
            Ok(()) => {},
            Err(_) => {}
        }
    }

}
