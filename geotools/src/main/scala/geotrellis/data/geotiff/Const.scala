package geotrellis.data.geotiff

/**
 * This object is contains useful geotiff-related constants.
 *
 * The first group of constants relates to parsing Tiff tags, and the second
 * group relates to Tiff sample formats.
 */
object Const {
  final def uint8 = 1      // 8-bit unsigned int
  final def ascii = 2      // 7-bit ascii code (must end in NUL)
  final def uint16 = 3     // 16-bit unsigned int
  final def uint32 = 4     // 32-bit unsigned int
  final def rational = 5   // rational: 2 uint32 values (numerator/denominator)
  final def sint8 = 6      // 8-bit signed int
  final def undef8 = 7     // undefined 8-bit
  final def sint16 = 8     // 16-bit signed int
  final def sint32 = 9     // 32-bit signed int
  final def srational = 10 // signed rational: 2 sint32 values (n/d)
  final def float32 = 11   // 32-bit floating point
  final def float64 = 12   // 64-bit floating point

  final def unsigned = 1 // unsigned integer samples
  final def signed = 2   // signed integer samples
  final def floating = 3 // floating-point samples
}
