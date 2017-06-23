package main;

import "flag";

type options struct {
  debug bool;
  conf string;
}

var opts options;

func init() {
  flag.BoolVar(&opts.debug, "debug", false, "print debug msg");
  flag.StringVar(&opts.conf, "conf", "conf/default.json", "path to conf file");
}

