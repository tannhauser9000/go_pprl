package numbers;

import "encoding/binary";

func B2Uint16L(input []byte) (uint16){
  var output uint16
  var i int
  var tmp uint16
  output = uint16(0)
  for i = 0; i < 2; i++{
    tmp = uint16(input[1 - i]);
    output = output << 8
    output = output + tmp
  }
  return output
}

// tannhauser, transform an uint16 into a byte[] of length 2
func Ui16ToBL(input uint16) ([]byte){
  output := make([]byte, 2)
  binary.LittleEndian.PutUint16(output, input)
  return output
}

// tannhauser, transform a byte[] of length 4 into a uint32
func B2Uint32L(input []byte) (uint32){
  var output uint32
  var i int
  var tmp uint32
  output = uint32(0)
  for i = 0; i < 4; i++{
    tmp = uint32(input[3 - i])
    output = output << 8
    output = output + tmp
  }
  return output
}

// tannhauser, transform an uint32 into a byte[] of length 4
func Ui32ToBL(input uint32) ([]byte){
  output := make([]byte, 4)
  binary.LittleEndian.PutUint32(output, input)
  return output
}

// tannhauser, transform a byte[] of length 8 into a uint64
func B2Uint64L(input []byte) (uint64){
  var output uint64
  var i int
  var tmp uint64
  output = uint64(0)
  for i = 0; i < 8; i++{
    tmp = uint64(input[7 - i])
    output = output << 8
    output = output + tmp
  }
  return output
}

// tannhauser, transform an uint64 into a byte[] of length 8
func Ui64ToBL(input uint64) ([]byte){
  output := make([]byte, 8)
  binary.LittleEndian.PutUint64(output, input)
  return output
}

// tannhauser, transform a byte[] of length 4 into a uint32
func B2Uint32(input []byte) (uint32){
  var output uint32
  var i int
  var tmp uint32
  output = uint32(0)
  for i = 0; i < 4; i++{
    tmp = uint32(input[i])
    output = output << 8
    output = output + tmp
  }
  return output
}

// tannhauser, transform an uint32 into a byte[] of length 4
func Ui32ToB(input uint32) ([]byte){
  output := make([]byte, 4)
  binary.BigEndian.PutUint32(output, input)
  return output
}

// tannhauser, transform a byte[] of length 8 into a uint64
func B2Uint64(input []byte) (uint64){
  var output uint64
  var i int
  var tmp uint64
  output = uint64(0)
  for i = 0; i < 8; i++{
    tmp = uint64(input[i])
    output = output << 8
    output = output + tmp
  }
  return output
}

// tannhauser, transform an uint64 into a byte[] of length 8
func Ui64ToB(input uint64) ([]byte){
  output := make([]byte, 8)
  binary.BigEndian.PutUint64(output, input)
  return output
}

