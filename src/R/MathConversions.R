#' Get the integer part of a floating point number
#' @description
#' Function that calculates the integer part of a floating point number.
#' Handles positive and negative numbers.
#' @param x Numeric
#' @return Integer part
#' @export
#' @examples
#' intPart(1.5)
#' intPart(1.4)
#' intPart(-1.5)
#' intPart(-1.4)

intPart <- function(x)
{
  y <- floor(x)

  return (y)
}

#' Get the fractional part of a floating point number
#' @description
#' Function that calculates the fractional part of a floating point number.
#' Handles positive and negative numbers.
#' @param x Numeric
#' @return Fractional part
#' @export
#' @examples
#' fracPart(1.5)
#' fracPart(-1.5)

fracPart <- function(x)
{
  y <- 0
  if (x <= 0) {
    y <- x - intPart(x)
  } else {
    y <- abs(x) - intPart(abs(x))
  }

  return (y)
}
