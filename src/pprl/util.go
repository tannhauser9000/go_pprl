package pprl;

import "fmt";
import "math";
import "log";
import "sync";

/* prepare dataset and meta data */
func(cf *Config) PrepareDataset() (error) {
  var err error;
  log.Printf("[PrepareDataset] Loading datasets...\n");
  for i, d := range (*cf).dataset {
    if (*cf).debug {
      log.Printf("[PrepareDataset] preparing dataset %d\n", i);
    }
    err = d.prepare_dataset();
    if err != nil {
      return err;
    }
    if (*cf).debug {
      log.Printf("[PrepareDataset] dataset %d prepared\n", i);
    }
  }
  log.Printf("[PrepareDataset] Calculating weights...\n");
  if err = cf.weight_entropy(); err != nil {
    return err;
  }
  log.Printf("[PrepareDataset] Preparing encoding...\n");
  if err := cf.prepare_encoding(); err != nil {
    return err;
  }
  log.Printf("[PrepareDataset] Setting bloom filters...\n");
  if err = (*cf).set_bloom_filter(); err != nil {
    return err;
  }
  return nil;
}

/* prepare a single dataset */
func (d *Dataset) prepare_dataset() (error) {
  d.entropy();
  d.ngram();
  return nil;
}

/* display field metadata */
func (cf *Config) PrintMeta() {
  for i := 0; i < (*(*cf).Nf); i++ {
    fmt.Printf("Weight of field %d: %f\n", i, (*cf).weight[i]);
  }
  for i := 0; i < (*(*cf).Nf); i++ {
    fmt.Printf("k, m, g of field %d: %d, %d, %f\n", i, (*cf).k[i], (*cf).m[i], (*cf).g[i]);
  }
  for i := 0; i < (*cf).nd; i++ {
    fmt.Printf("Printing metadata for dataset %d...\n", i);
    (*cf).dataset[i].print_meta();
  }
}

/* dataset-wise metadata display */
func (d *Dataset) print_meta() {
  for i := 0; i < (*(*d).nf); i++ {
    if (*(*d).ignore)[i] {
      fmt.Printf("[%s] field ignored.\n", (*(*d).field[i]).name);
    } else {
      fmt.Printf("[%s] entropy: %f, avg_n_gram: %d\n", (*(*d).field[i]).name, (*(*d).field[i]).entropy, (*d).g[i]);
    }
  }
}

/* calculate entropy */
func (d *Dataset) entropy() {
  var field *FieldMeta;
  var wg sync.WaitGroup;
  var ent, prob, total float64;

  /* first pass, array version */
  for i := 0; i < (*(*d).nf); i++ {
    wg.Add(1);
    go go_first_pass(d, i, &wg);
  }
  wg.Wait();

  /* second pass, entropy calculation */
  for i := 0; i < (*(*d).nf); i++ {
    if !(*(*d).ignore)[i] {
      field = (*d).field[i];
      total = (*field).total;
      (*field).entropy = 0;
      for _, v := range (*field).freq {
        if v != 0 {
          prob = v / total;
          ent = math.Log2(prob);
          (*field).entropy -= prob * ent;
          /*
          if (*(*d).debug) {
            log.Printf("[entropy] %s: %f=%f/%f, %f\n", k, prob, v, total, ent);
          }
          //*/
        }
      }
    }
  }
}

/* dispatch parse items to corresponding location */
func go_first_pass(ds *Dataset, this int, w *sync.WaitGroup) {
  var cnt float64;
  var ok bool;
  /* if this field is ignored */
  if (*(*ds).ignore)[this] {
    (*w).Done();
    return;
  }
  /* get a go routine */
  go_routine := get_go();
  defer func() {
    go_routine.free_go();
    (*w).Done();
  } ();
  //one_percent := (*ds).nr / 100;
  field := (*ds).field[this];
  (*field).exists = 0;
  (*field).total = 0;
  var raw string;
  for j := 0; j < (*ds).nr; j++ {
    if (*ds).record[j] == nil {
      log.Printf("[PPRL][go_first_pass] record %d is nil\n", j);
    }
    if (*(*ds).record[j]).field[this] == nil {
      log.Printf("[PPRL][go_first_pass] field %s (%d) of record %d is nil\n", (*(*ds).field[this]).name, this, j);
    }
    raw = (*(*(*ds).record[j]).field[this]).raw;
    if raw != "n/a" {
      cnt, ok = (*field).freq[raw];
      if !ok {
        (*field).freq[raw] = float64(1);
      } else {
        (*field).freq[raw] = cnt + float64(1);
      }
      (*field).exists++;
    }
    (*field).total++;
    /*
    if int((*field).total) % one_percent == 0 {
      percent := float64((*field).total)/float64((*ds).nr)*float64(100);
      log.Printf("%.2f%% of field [%s] in first pass...\n", percent, (*(*ds).field[this]).name);
    }
    //*/
  }
}

/* controller n-gram calculation */
func (d *Dataset) ngram() {
  var wg sync.WaitGroup;
  for i := 0; i < (*d).nr; i++ {
    for j := 0; j < (*(*d).nf); j++ {
      if !(*(*d).ignore)[j] {
        wg.Add(1);
        go (*(*d).record[i]).field[j].make_ngram(&wg);
      }
    }
  }
  wg.Wait();
}

/* n-gram calculation */
func (f *Field) make_ngram(wg *sync.WaitGroup) {
  /* get a go routine */
  go_routine := get_go();
  defer func() {
    go_routine.free_go();
    (*wg).Done();
  } ();
  padding_len := (*(*f).ng) - 1;
  ngram_len := len((*f).raw) - padding_len;
  (*f).ngram = make([]string, ngram_len);
  for i := 0; i < ngram_len; i++ {
    (*f).ngram[i] = (*f).padded[i:i + padding_len];
  }
}

/* encode to bloom filter */
func (d *Dataset) Encoding() (error) {
  return nil;
}



