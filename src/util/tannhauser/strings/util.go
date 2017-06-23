package strings;

func Split(src, sep string, dst *[]string, str_index *int) {
  head := sep[0];
  start := 0;
  *str_index = 0;
  n := len(*dst);
  for i := 0; i + len(sep) <= len(src) && *str_index+1 < n; i++ {
    if src[i] == head && (len(sep) == 1 || src[i:i+len(sep)] == sep) {
      (*dst)[*str_index] = src[start : i];
      *str_index++;
      start = i + len(sep);
      i += len(sep) - 1;
    }
  }
  (*dst)[*str_index] = src[start:]
  *str_index++;
}

