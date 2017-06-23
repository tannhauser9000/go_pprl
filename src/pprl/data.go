package pprl;

import "bufio";
import "crypto/md5";
import "hash";
import "log";
import "math";
import "os";
import "strconv";
import "strings";
import "sync";

import "util/tannhauser/config";
import "util/tannhauser/pool";
import tstrings "util/tannhauser/strings";

type Config struct {
  /* exported fields, for JSON config */
  Dataset string `json:"dataset"`;  // path to datasets, separated by ","
  Prefix string `json:"prefix"`;    // prefix to datasets
  Size string `json:"size"`;        // #record for each dataset, separated by ","
  Ignore string `json:"ignore"`;    // index of field to be ignored, separated by ",", begins from 0
  Buffer int `json:"buffer"`;       // #buffer in resource pool
  Hash int `json:"hash"`;           // #hash buffer in resource pool
  Nf *int `json:"num_field"`;       // #fields
  Ng *int `json:"ngram"`;           // n for n-gram
  Mb int `json:"bloom_bit"`;        // #bit in the result bloom filter
  Blk int `json:"block_bit"`;       // #bit for block
  MaxGo int `json:"max_routine"`;   // max number of go routine
  Ratio *float64 `json:"ratio"`;     // ratio of on bits in resulting bloom filter

  /* data instance */
  fp []*os.File;                    // file pointer for datasets
  dataset []*Dataset;               // datasets
  weight []float64;                 // final weight of each field

  /* conf */
  conf string;                      // path to config file
  debug bool;                       // debug mode
  buffer_pool int;                  // size limit on buffer pool
  hash_pool int;                    // size limit on hash pool
  nd int;                           // #dataset
  path []string;                    // array of relative path to dataset
  ignore []bool;                    // array of ignored field indexes
  g []float64;                      // array of average n gram length for each field of each dataset
  k []int;                          // array of #hash for each field
  m []int;                          // array of #m-bits for each field
}

type Dataset struct {
  /* data to be exposed */
  Discriminatory []*float64;    // discriminatory powa~~~
  Entropy []*float64;           // entropy
  Weight *[]float64;            // weight

  /* internal data */
  record []*Record;             // raw data
  field []*FieldMeta;           // array of field datas
  nr int;                       // #record
  nf *int;                      // #field
  ignore *[]bool;               // pointer to Dataset.ignore
  debug *bool;                  // pointer to Config.debug
  g []float64;                  // array of average n gram length for each field
}

type Record struct {
  field []*Field;               // field data
  bloom_filter []byte           // bloom filter
  block []int                   // block number
}

type Field struct {
  raw string;                   // raw data
  padded string;                // padded data for n_gram
  ngram []string;               // array of ngrams
  bf_index [][]int;             // index of bloom table, [k_i][n_gram_i]
  ng *int;                      // n of n-gram
  mb *int;                      // pointer to Config.Mb
}

type FieldMeta struct {
  name string;                  // field name
  index int;                    // array index in Dataset.fields
  freq map[string]float64;      // field frequence
  exists float64;               // #records with the certain field
  total float64;                // #records
  discriminatory float64;       // discriminatory powa~~~
  entropy float64;              // raw entropy
  weight *float64;              // field weight
  avg_n_gram *float64;          // average n gram length, take ceilling
  sum_n_gram float64;           // sum of n gram length for the certain field
}

type Buffers struct {
  index *pool.IndexPool;
  buffer []*Buffer;
}

type Buffer struct {
  Index int;
  field_buffer []string;
}

type Hashes struct {
  index *pool.IndexPool;
  hash []*Hash;
}

type Hash struct {
  Index int;
  hash hash.Hash;               // actual hash instance
}

type go_pool_st struct {
  index *pool.IndexPool;
  pool []*go_st;
}

type go_st struct {
  index int;
}

type progress_wait_group struct {
  total int32;
  wg sync.WaitGroup;
}

type Error string;

/* constant error */
const ErrConfigSizeNotMatch = Error("#size should equal to #dataset");
const ErrNf = Error("missing or invalid num_field");
const ErrSize = Error("invalid size value");
const ErrInvalidIgnore = Error("invalid ignore index");
const ErrMb = Error("#bit larger than predefined #bit of bloom filter");
const ErrMbDistribution = Error("failed to redistribute remaining bits");

/* default configs */
const _default_buffer_pool = 10;
const _default_hash_pool = 128;
const _default_ngram = 2;
const _default_mb = 1024;
const _default_block = 4;
const _default_go_routine = 4096;
const _default_ratio = float64(0.5);

/* internal structure */
const _padding_tbl_size = 11;

/* global variables */
var buffers Buffers;
var hashes Hashes;
var bloom_table [][]byte;
var go_pool go_pool_st;
var padding_tbl []string;

/* for constant error */
func (e Error) Error() (string) {
  return string(e);
}

/* get default config */
func InitConfig(path string, debug bool) (*Config, error) {
  cf := &Config {
    conf: path,
    debug: debug,
  };
  err := cf.init_resource();
  return cf, err;
}

/* init functions */
func (cf *Config) init_resource() (error) {
  var err error;
  if err = cf.init_config(); err != nil {
    return err;
  }
  if err = cf.init_buffers(); err != nil {
    return err;
  }
  if err = cf.init_dataset(); err != nil {
    return err;
  }
  return nil;
}

/* load config */
func (cf *Config) init_config() (error) {
  err := config.InitJSONConf((*cf).conf, cf);
  if err != nil {
    return err;
  }
  if (*cf).debug {
    log.Printf("[PPRL][init_config] config: %v\n", (*cf));
  }

  /* value check */
  if (*cf).Buffer <= 0 {
    (*cf).buffer_pool = _default_buffer_pool;
  } else {
    (*cf).buffer_pool = (*cf).Buffer;
  }
  if (*cf).Hash <= 0 {
    (*cf).hash_pool = _default_hash_pool;
  } else {
    (*cf).hash_pool = (*cf).Hash;
  }
  (*cf).path = strings.Split((*cf).Dataset, ",");
  (*cf).nd = len((*cf).path);
  sizes := strings.Split((*cf).Size, ",");
  if len(sizes) != (*cf).nd {
    return ErrConfigSizeNotMatch;
  }
  if (*cf).Nf == nil || (*(*cf).Nf) == 0 {
    return ErrNf;
  }
  if (*cf).Ng == nil || (*(*cf).Ng) == 0 {
    dn := _default_ngram;
    (*cf).Ng = &dn;
  }
  if (*cf).Mb == 0 {
    (*cf).Mb = _default_mb;
  }
  if (*cf).Blk == 0 {
    (*cf).Blk = _default_block;
  }
  if (*cf).MaxGo == 0 {
    (*cf).MaxGo = _default_go_routine;
  }
  if (*cf).Ratio == nil || (*(*cf).Ratio) == 0 {
    ratio := _default_ratio
    (*cf).Ratio = &ratio;
  }
  ignore := strings.Split((*cf).Ignore, ",");
  if len(ignore) >= (*(*cf).Nf) {
    return ErrInvalidIgnore;
  }
  (*cf).ignore = make([]bool, (*(*cf).Nf));
  tmp_ignore := 0;
  for i := 0; i < len(ignore); i++ {
    tmp_ignore, err = strconv.Atoi(ignore[i]);
    if err != nil || tmp_ignore < 0 || tmp_ignore >= (*(*cf).Nf) {
      return ErrInvalidIgnore;
    }
    (*cf).ignore[tmp_ignore] = true;
  }
  /* malloc and set dataset */
  (*cf).fp = make([]*os.File, (*cf).nd);
  (*cf).dataset = make([]*Dataset, (*cf).nd);
  (*cf).g = make([]float64, (*(*cf).Nf));
  (*cf).k = make([]int, (*(*cf).Nf));
  (*cf).m = make([]int, (*(*cf).Nf));
  for i := 0; i < (*cf).nd; i++ {
    size, err := strconv.Atoi(sizes[i]);
    if err != nil {
      return ErrSize;
    }
    (*cf).dataset[i] = &Dataset {
      Discriminatory: make([]*float64, (*(*cf).Nf)),
      Entropy: make([]*float64, (*(*cf).Nf)),
      //Weight: make([]*float64, (*(*cf).Nf)),
      Weight: &(*cf).weight,

      record: make([]*Record, size),
      nr: size,
      nf: (*cf).Nf,
      ignore: &((*cf).ignore),
      debug: &(*cf).debug,
    };
  }
  (*cf).weight = make([]float64, (*(*cf).Nf));
  return nil;
}

/* init buffers */
func (cf *Config) init_buffers() (error) {
  /* initialize necessary buffers */
  buffers = Buffers {
    index: &pool.IndexPool{},
    buffer: make([]*Buffer, (*cf).buffer_pool),
  };
  buffers.index.InitIndexPool((*cf).buffer_pool);
  var tmp_str_ary []string;
  for i := 0; i < (*cf).buffer_pool; i++ {
    tmp_str_ary = make([]string, (*(*cf).Nf));
    buffers.buffer[i] = &Buffer {
      Index: i,
      field_buffer: tmp_str_ary,
    };
  }
  /* initialize paddings */
  padding_tbl = make([]string, _padding_tbl_size);
  padding := "";
  for i := 0; i < _padding_tbl_size; i++ {
    padding_tbl[i] = padding;
    padding += "*";
  }
  /* initialize hashes */
  hashes = Hashes {
    index: &pool.IndexPool{},
    hash: make([]*Hash, (*cf).hash_pool),
  };
  for i := 0; i < (*cf).hash_pool; i++ {
    hashes.hash[i] = &Hash {
      Index: i,
      hash: md5.New(),
    };
  }
  /* create basic bloom table */
  basic_bloom := make([]byte, 8);
  for i := 0; i < 8; i++ {
    basic_bloom[i] = uint8(1) << uint8(i);
  }
  bloom_table = make([][]byte, (*cf).Mb);
  bf_bytes := (*cf).Mb / 8;
  if (*cf).Mb % 8 != 0 {
    bf_bytes++;
  }
  slot_byte := 0;
  slot_basic := 0;
  for i := 0; i < (*cf).Mb; i++ {
    bloom_table[i] = make([]byte, bf_bytes);
    slot_byte = i / 8;
    slot_basic = i % 8;
    bloom_table[i][slot_byte] = basic_bloom[slot_basic];
  }
  /* create go_routine pool */
  go_pool = go_pool_st {
    index: &pool.IndexPool{},
    pool: make([]*go_st, (*cf).MaxGo),
  }
  go_pool.index.InitIndexPool((*cf).MaxGo);
  for i := 0; i < (*cf).MaxGo; i++ {
    go_pool.pool[i] = &go_st {
      index: i,
    };
  }
  return nil;
}

/* get a buffer */
func get_buffer() (*Buffer) {
  index := buffers.index.GetIndex();
  (*(buffers.buffer[index])).Index = index;
  return buffers.buffer[index];
}

/* free a buffer */
func (b *Buffer) free_buffer() {
  index := (*b).Index;
  buffers.index.FreeIndex(index);
  return;
}

/* get a hash */
func get_hash() (*Hash) {
  index := hashes.index.GetIndex();
  (*(hashes.hash[index])).Index = index;
  return hashes.hash[index];
}

/* free a buffer */
func (h *Hash) free_hash() {
  index := (*h).Index;
  hashes.index.FreeIndex(index);
  return;
}

/* get a go routine */
func get_go() (*go_st) {
  index := go_pool.index.GetIndex();
  (*go_pool.pool[index]).index = index;
  return go_pool.pool[index];
}

/* free a go routine */
func (g *go_st) free_go() {
  index := (*g).index;
  go_pool.index.FreeIndex(index);
  return;
}

/* load datasets */
func (cf *Config) init_dataset() (error) {
  var err error;
  /* open files */
  defer (*cf).finish();
  for i := 0; i < (*cf).nd; i++ {
    (*cf).fp[i], err = os.Open((*cf).Prefix + "/" + (*cf).path[i]);
    if err != nil {
      return err;
    }
  }
  return cf.load_datasets();
}

/* load dataset from file */
func (cf *Config) load_datasets() (error) {
  var wg sync.WaitGroup;
  for i := 0; i < (*cf).nd; i++ {
    wg.Add(1);
    go load_single_dataset(cf, i, &wg);
  }
  wg.Wait();
  /* average len(n_gram) of each field of the datasets */
  sum := make([]float64, (*(*cf).Nf));
  sum_nr := float64(0);
  var d *Dataset;
  for i := 0; i < (*cf).nd; i++ {
    d = (*cf).dataset[i];
    sum_nr += float64((*d).nr);
    for j := 0; j < (*(*cf).Nf); j++ {
      sum[j] += (*d).g[j] * float64((*d).nr);
    }
  }
  for i := 0; i < (*(*cf).Nf); i++ {
    (*cf).g[i] = math.Ceil(sum[i] / sum_nr);
  }
  return nil;
}

/* load single dataset from file */
func load_single_dataset(cf *Config, this int, wg *sync.WaitGroup) {
  /* get a go routine */
  go_routine := get_go();
  /* preparing variables */
  var raw_record string;
  padding := "";
  for i := 0; i < (*(*cf).Ng) - 1; i++ {
    padding = padding + " ";
  }
  /* for each dataset, use go routine to initialize corresponding resources */
  dataset := (*cf).dataset[this];
  defer func() {
    go_routine.free_go();
    (*wg).Done();
  } ();
  scanner := bufio.NewScanner((*cf).fp[this]);
  if (*cf).debug {
    log.Printf("[PPRL][load_datasets] scanner: %v\n", scanner);
  }
  cnt := 0;
  dummy_nf := 0;    // dummy value for tstrings.Split
  var buffer *Buffer;
  for scanner.Scan() {
    buffer = get_buffer();
    raw_record = scanner.Text();
    ///*
    if (*cf).debug {
      log.Printf("[PPRL][load_datasets] %d/%d: %s\n", this, cnt, raw_record);
    }
    //*/
    tstrings.Split(raw_record, ",", &(*buffer).field_buffer, &dummy_nf);
    if cnt == 0 {
      /* memory allocation */
      (*dataset).field = make([]*FieldMeta, (*(*cf).Nf));
      (*dataset).record = make([]*Record, (*dataset).nr);
      (*dataset).g = make([]float64, (*(*cf).Nf));
      for i := 0; i < (*(*cf).Nf); i++ {
        /* head line, initialize FieldMeta */
        (*dataset).field[i] = &FieldMeta {
          name: strings.TrimSpace((*buffer).field_buffer[i]),
          index: i,
          freq: make(map[string]float64),
          exists: 0,
          total: 0,
          discriminatory: 0,
          entropy: 0,
          weight: &((*cf).weight[i]),
          avg_n_gram: &(*dataset).g[i],
        };

        /* link discriminatory/entropy array in Dataset to FieldMeta */
        (*dataset).Discriminatory[i] = &((*(*dataset).field[i]).discriminatory);
        (*dataset).Entropy[i] = &((*(*dataset).field[i]).entropy);

      }
      /* allocate Record */
      (*dataset).record = make([]*Record, (*dataset).nr);
    } else {
      /* record lines, fillup Record */
      record := Record {
        field: make([]*Field, (*(*cf).Nf)),
        bloom_filter: make([]byte, (*cf).Mb),
        block: make([]int, 1 << uint32((*cf).Blk)),
      };
      (*dataset).record[cnt - 1] = &record;
      for i := 0; i < (*(*cf).Nf); i++ {
        (*(*dataset).field[i]).total++;
        raw := strings.TrimSpace((*buffer).field_buffer[i]);
        padded := raw;
        if raw != "" {
          padded = padding + padded + padding;
          (*(*dataset).field[i]).exists++;
          (*(*dataset).field[i]).sum_n_gram += float64(len(padded) - (*(*cf).Ng) + 1);
        } else {
          raw = "n/a";
          padded = " " + padding;
        }
        record.field[i] = &Field {
          raw: raw,
          padded: padded,
          ng: (*cf).Ng,
          mb: &(*cf).Mb,
          //bf_index has to be decided once k and n_gram are calculated
        };
      }
    }
    cnt++;
    buffer.free_buffer();
  }
  var f *FieldMeta;
  for i := 0; i < (*(*cf).Nf); i++ {
    f = (*dataset).field[i];
    if !(*cf).ignore[i] {
      (*(*f).avg_n_gram) = math.Ceil((*f).sum_n_gram / (*f).exists);
    }
  }
  if cnt - 1 != (*dataset).nr {
    log.Printf("[PPRL][InitConfig] #record in config is %d, differ from dataset file (%d)\n", (*dataset).nr, cnt - 1);
  }
}

/* close file pointers in Config */
func (cf *Config) finish() {
  for i := 0; i < (*cf).nd; i++ {
    if (*cf).fp[i] != nil {
      (*cf).fp[i].Close();
    }
  }
}

