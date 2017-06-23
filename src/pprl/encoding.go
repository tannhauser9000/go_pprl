package pprl;

import "crypto/md5";
import "io";
import "math";
import "sync";

import "util/tannhauser/numbers";

/* distribute bloom filter bits to each field and calculate parameters for encoding */
func (cf *Config) prepare_encoding() (error) {
  /* first distribution */
  sum := 0;
  for i := 0; i < (*(*cf).Nf); i++ {
    (*cf).m[i] = int(math.Floor(float64((*cf).Mb) * (*cf).weight[i]));
    sum += (*cf).m[i];
  }
  /* distribute remaining */
  dif := (*cf).Mb - sum;
  if dif < 0 {
    return ErrMb;
  }
  dispatched := 0;
  if dif > 0 {
    distributed := make([]bool, (*(*cf).Nf));
    min := 0;
    index := 0;
    for i := 0; i < dif; i++ {
      min = 0;
      for j := 0; j < (*(*cf).Nf); j++ {
        if !(*cf).ignore[j] {
          if min == 0 || (*cf).m[i] < min && !distributed[i] {
            min = (*cf).m[i];
            index = i;
          }
        }
      }
      (*cf).m[index]++;
      distributed[index] = true;
      dispatched++;
      if dispatched % (*(*cf).Nf) == 0 {
        for j := 0; j < (*(*cf).Nf); j++ {
          distributed[j] = false;
        }
      }
    }
  }
  /* check sum of #bit */
  sum = 0;
  for i := 0; i < (*(*cf).Nf); i++ {
    sum += (*cf).m[i];
  }
  dif = (*cf).Mb - sum;
  if dif != 0 {
    return ErrMbDistribution;
  }

  /* calculate k_i */
  p := math.Log2(*(*cf).Ratio);
  m := float64(0);
  for i := 0; i < (*(*cf).Nf); i++ {
    if !(*cf).ignore[i] {
      m = (float64((*cf).m[i]) - 1)/float64((*cf).m[i]);
      num_hash := (p/math.Log2(m))/(*cf).g[i];
      (*cf).k[i] = int(num_hash);
      /* allocate bf_index for each field of each record */
      for j := 0; j < (*cf).nd; j++ {
        d := (*cf).dataset[j];
        for k := 0; k < (*d).nr; k++ {
          f := (*(*d).record[k]).field[i];
          (*f).bf_index = make([][]int, (*cf).k[i]);
          for l := 0; l < (*cf).k[i]; l++ {
            (*f).bf_index[l] = make([]int, len((*f).ngram));
          }
        }
      }
    }
  }

  return nil;
}

/* set bloom filters */
func (cf *Config) set_bloom_filter() (error) {
  if err := cf.set_bloom_index(); err != nil {
    return err;
  }
  return nil;
}

/* set bloom table index */
func (cf *Config) set_bloom_index() (error) {
  /* use go routine for parallel processing */
  wg := sync.WaitGroup{};
  /* get bloom filter indexes */
  for i := 0; i < (*cf).nd; i++ {
    d := (*cf).dataset[i];
    for j := 0; j < (*d).nr; j++ {
      record := (*d).record[j];
      for k := 0; k < (*(*d).nf); k++ {
        f := (*record).field[k];
        wg.Add(1);
        go f.get_bloom_index(&(*cf).k[k], &wg);
      }
    }
  }
  wg.Wait();
  return nil;
}

/* get bloom table index for specific field of certain record */
func (f *Field) get_bloom_index(method *int, wg *sync.WaitGroup) {
  for i := 0; i < len((*f).ngram); i++ {
    for j := 0; j < (*method); j++ {
      get_index(&(*f).ngram[i], &(*f).bf_index[j][i], &j, (*f).mb);
    }
  }
  (*wg).Done();
  return;
}

/* get bloom table index from the given input string pointer */
func get_index(in *string, out *int, method *int, mb *int) {
  h := get_hash()
  hash_value := h.get_hash_value(in, method);
  h.free_hash();
  hash_to_index(out, mb, &hash_value);
}

/* transform hash value to bloom table index */
func hash_to_index(out, mb *int, value *[]byte) {
  num_1 := numbers.B2Uint64L((*value)[:8]);
  num_2 := numbers.B2Uint64L((*value)[8:md5.Size]);
  index := num_1 ^ num_2;
  (*out) = int(index % uint64(*mb));
}

/* get hash value from input string pointer and specified hash method */
func (h *Hash) get_hash_value(in *string, method *int) ([]byte) {
  (*h).hash.Reset();
  hash_input := get_padding(in, method);
  io.WriteString((*h).hash, (*hash_input));
  return (*h).hash.Sum(nil);
}

/* get string padding for specified hash method */
func get_padding(in *string, method *int) (*string) {
  remain := (*method);
  padding := "";
  /* use _padding_tbl_size = 11 */
  for {
    if remain >= 10 {
      padding += padding_tbl[10];
      remain -= 10;
    }
    if remain >= 9 {
      padding += padding_tbl[9];
      remain -= 9;
    }
    if remain >= 8 {
      padding += padding_tbl[8];
      remain -= 8;
    }
    if remain >= 7 {
      padding += padding_tbl[7];
      remain -= 7;
    }
    if remain >= 6 {
      padding += padding_tbl[6];
      remain -= 6;
    }
    if remain >= 5 {
      padding += padding_tbl[5];
      remain -= 5;
    }
    if remain >= 4 {
      padding += padding_tbl[4];
      remain -= 4;
    }
    if remain >= 3 {
      padding += padding_tbl[3];
      remain -= 3;
    }
    if remain >= 2 {
      padding += padding_tbl[2];
      remain -= 2;
    }
    if remain >= 1 {
      padding += padding_tbl[1];
      remain -= 1;
    }
    if remain == 0 {
      padded := padding + (*in);
      return &padded;
    }
  }
}

