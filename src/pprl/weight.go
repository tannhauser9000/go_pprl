package pprl;

func (cf *Config) weight_entropy() (error) {
  var d *Dataset;
  sum_entropy := make([]float64, (*(*cf).Nf));
  sum := float64(0);
  this := float64(0);
  for i := 0; i < (*cf).nd; i++ {
    d = (*cf).dataset[i];
    for j := 0; j < (*(*cf).Nf); j++ {
      /* calculate weight weighting, currently use (nr * entropy)/sum(nr * entropy) for all datasets */
      if !(*cf).ignore[i] {
        this = float64((*d).nr) * (*(*d).field[j]).entropy;
        sum += this;
        sum_entropy[j] += this;
      }
    }
  }
  for i := 0; i < (*(*cf).Nf); i++ {
    (*cf).weight[i] = sum_entropy[i] / sum;
  }
  return nil;
}


