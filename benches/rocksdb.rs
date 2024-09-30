use criterion::{criterion_group, criterion_main, Criterion};
use rand::distributions::WeightedIndex;
use rand_chacha::ChaCha20Rng;
use rocksdb::{self, ColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, Error, Options, SingleThreaded, DB};
use rand::prelude::*;

type Storage = DBWithThreadMode<SingleThreaded>;

fn setup(path: &str, cf_name: &str, warm_up_size: usize, val_size: usize) -> Result<Storage, Error>
{
    let mut cf_opts = Options::default();
    cf_opts.set_max_write_buffer_number(16);
    let cf = ColumnFamilyDescriptor::new(cf_name, cf_opts);
    
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);
    let db = DB::open_cf_descriptors(&db_opts, path, vec![cf]).unwrap();

    let cf = db.cf_handle(cf_name).unwrap();
    let mut key_cnt = 0;
    for _ in 0..warm_up_size {
        put_bench(&db, cf, &mut key_cnt, val_size);
    }


    Ok(db)
}

fn put_bench(db: &Storage, cf: &ColumnFamily, key_cnt: &mut u32, val_size: usize) {
    let key = format!("key:{}", key_cnt);
    let key = key.as_bytes();

    let val = vec!['a' as u8; val_size];
    let val = val.as_slice();

    let _ = db.put_cf(cf, key, val);
    *key_cnt += 1;
}

fn get_bench(db: &Storage, cf: &ColumnFamily, key_cnt: u32) {
    let key = format!("key:{}", key_cnt);
    let key = key.as_bytes();

    let _ = db.get_cf(cf, key);
}

// fn rw_bench(db: &mut Storage, cf: &ColumnFamily, write_ratio: f32, total_runs: usize) {
fn rand_setup(write_ratio: f32) -> (ChaCha20Rng, [(bool, i32); 2], WeightedIndex<i32>) {
    let rng = ChaCha20Rng::seed_from_u64(42);
    let sample_item = [(true, (write_ratio * 100.0) as i32), (false, ((1.0 - write_ratio) * 100.0) as i32)];
    (rng, sample_item, WeightedIndex::new(sample_item.iter().map(|(_, weight)| weight)).unwrap())
}

fn compound_bench(rng: &mut ChaCha20Rng, sample_item: &[(bool, i32); 2], weight_dist: &WeightedIndex<i32>, key_cnt: &mut u32, val_size: usize, db: &Storage, cf: &ColumnFamily) {
    let should_write = sample_item[weight_dist.sample(rng)].0;

    if should_write {
        put_bench(db, cf, key_cnt, val_size);
    } else {
        let read_cnt: u32 = rng.gen();
        get_bench(db, cf, read_cnt % *key_cnt);
    }

}

fn criterion_benchmark(c: &mut Criterion) -> Result<(), Error> {
    let payload_sizes: &[usize] = &[64, 256, 1024, 8192, 1048576];
    let write_ratios: &[f32] = &[0.0, 0.5, 1.0];
    
    let path = "/tmp/testdb";
    let cf_name = "cf1";
    let warm_up_size = 100;

    let mut group = c.benchmark_group("Rw_bench");
    for &payload in payload_sizes {
        for &write_ratio in write_ratios {
            let db = setup(path, cf_name, warm_up_size, payload)?;
            let cf = db.cf_handle(cf_name).unwrap();
            let mut key_cnt = warm_up_size as u32;
            let (mut rng, sample_item, weighted_dist) = rand_setup(write_ratio);
            group.bench_function(format!("write_ratio={}/val_size={}", write_ratio, payload), |b| b.iter(|| {
                compound_bench(&mut rng, &sample_item, &weighted_dist, &mut key_cnt, payload, &db, cf);
            }));
            let opts = Options::default();
            let _ = DB::destroy(&opts, path);
            
        }
    }

    Ok(())
    
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);