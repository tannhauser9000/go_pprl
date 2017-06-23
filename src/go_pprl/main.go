package main;

import "flag";
import "log";
import "os";

import "pprl";

func main() {
  flag.Parse();
  if flag.NArg() > 0 {
    flag.PrintDefaults();
    return;
  }
  log.Printf("[%s] initializing config...\n", os.Args[0]);
  conf, err := pprl.InitConfig(opts.conf, opts.debug);
  if err != nil {
    log.Printf("[%s] failed to initialize PPRL procedure: %s\n", os.Args[0], err.Error());
    return;
  }
  log.Printf("[%s] preparing datasets...\n", os.Args[0]);
  err = conf.PrepareDataset();
  if err != nil {
    log.Printf("[%s] failed to prepare dataset: %s\n", os.Args[0], err.Error());
    return;
  }
  if opts.debug {
    log.Printf("[%s] config content: %v\n", os.Args[0], conf);
  }
  conf.PrintMeta();
}
