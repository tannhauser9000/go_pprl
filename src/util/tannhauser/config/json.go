package config;

import "encoding/json";
import "os";

type Error string;

const ErrConf = Error("Failed to get conf file");
const ErrInvalidField = Error("Got invalid key in conf file");
const ErrInvalidValue = Error("Got invalid value in conf file");
const ErrInvalidType = Error("Got invalid type in conf file");

/* Initiate configuration, pass path to conf file and return a configuration entity */
func InitJSONConf(path string, conf interface{}) (error) {
  fp, err := os.Open(path);
  defer fp.Close();
  if err != nil {
    return err;
  }
  decoder := json.NewDecoder(fp);
  err = decoder.Decode(conf);
  if err != nil {
    return err;
  }
  return nil;
}

