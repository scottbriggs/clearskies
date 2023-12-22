#' Convert degrees, arc minutes, and arc seconds to decimal degrees.
#' @description
#' Function that returns decimal degrees when given degrees, ar cminutes,
#' and arc seconds. The function supports negative degrees, arc minutes, and
#' arc seconds.
#' @param deg Numeric
#' @param min Numeric
#' @param sec Numeric
#' @return Decimal degrees
#' @export
#' @examples
#' dmsToDeg(24, 13, 18)
#' dmsToDeg(- 24, 13, 18)
#' dmsToDeg(13, 4, 10)
#' dmsToDeg(0, -5, 30)
#' dmsToDeg(0, 0, 30)
#' dmsToDeg(0, 0, -30)
#'
DMSToDeg <- function(deg, min, sec)
{
  sg <- 0
  if (deg != 0) {
    sg <- sign(deg)
  } else if (deg == 0 && min != 0) {
    sg <- sign(min)
  } else if (deg == 0 && min == 0 && sec != 0) {
    sg <- sign(sec)
  }

  degrees <- abs(deg)
  minutes <- abs(min)
  seconds <- abs(sec)
  dm <- seconds / 60
  tot_min <- dm + minutes
  decDeg <- tot_min / 60
  decDeg <- decDeg + degrees
  decDeg <- sg * decDeg

  return (decDeg)
}

#' Convert decimal degrees to degrees, arc minutes, and arc seconds.
#' @description
#' Function that returns a vector of degrees, arc minutes, and
#' arc seconds when given decimal degrees. Negative and positive degrees
#' are supported.
#' @param decDeg Numeric
#' @return A vector of length 3
#' @export
#' @examples
#' degToDMS(0.508333)
#' degToDMS(-0.508333)
#' degToDMS(10.2958)
#' degToDMS(13.069444)

DegToDMS <- function(decDeg)
{
  sg <- sign(decDeg)

  Dec <- abs(decDeg)
  Degrees <- IntPart(Dec)
  Minutes <- IntPart(60 * FracPart(Dec))
  Seconds <- 60 * FracPart(60 * FracPart(Dec))

  Seconds <- round(Seconds, 2)

  if (Seconds == 60) {
    Seconds <- 0
    Minutes <- Minutes + 1
  }

  if (Minutes == 60) {
    Minutes <- 0
    Degrees <- Degrees + 1
  }

  if (Degrees != 0) {
    Degrees <- Degrees * sg
  } else if (Minutes != 0) {
      Minutes <- Minutes * sg
    } else if (Seconds != 0) {
      Seconds <- Seconds * sg
    }

  return (c(Degrees, Minutes, Seconds))
}

#' Convert decimal hours to hours, minutes, and seconds.
#' @description
#' Function to convert decimal hours to hours, minutes, and seconds. Hours must
#' be greater than or equal to 0 and less than or equal to 24.
#' @param decHr Numeric
#' @return A vector of length 3
#' @export
#' @examples
#' hourToHMS(20.352)
#' hourToHMS(5.7333)
#' hourToHMS(-1)
#' hourToHMS(25)

HourToHMS <- function(decHr)
{
  try(if(decHr < 0) stop("Hours must be >= 0"))
  try(if(decHr > 24) stop("Hours must be <= 24"))

  Hours <- IntPart(decHr)
  Minutes <- IntPart(60 * FracPart(decHr))
  Seconds <- 60 * FracPart(60 * FracPart(decHr))

  Seconds <- round(Seconds, 2)

  if (Seconds == 60) {
    Seconds <- 0
    Minutes <- Minutes + 1
  }

  if (Minutes == 60) {
    Minutes <- 0
    Hours <- Hours + 1
  }

  return (c(Hours, Minutes, Seconds))
}

#' Convert hours, minutes, and seconds to decimal hours.
#' @description
#' Function to convert hours, minutes, and seconds to decimal hours. Hours must
#' be greater than or equal to 0 and less than or equal to 24.
#' @param hours Numeric
#' @param minutes Numeric
#' @param seconds Numeric
#' @return decimal Hours
#' @export
#' @examples
#' hmsToHour(10, 25, 11)
#' hmsToHour(-2, 0, 0)
#' hmsToHour(25, 0, 0)

HMSToHour <- function(hours, minutes, seconds)
{
  try(if(hours < 0) stop("Hours must be >= 0"))
  try(if(hours > 24) stop("Hours must be <= 24"))

  hm <- seconds / 60
  tot_min <- hm + minutes
  decHr <- tot_min / 60
  decHr <- decHr + hours

  return (decHr)
}

#' Convert degrees to hours
#' @description
#' Function to convert decimal degrees to decimal hours. Degrees should always
#' be positive as hours must always be positive.
#' @param decDeg Numeric
#' @return decimal hours
#' @export
#' @examples
#' degToHour(156.3)
#' degToHour(90)
#' degToHour(180)
#' degToHour(270)
#' degToHour(-1)
#' degToHour(361)

DegToHour <- function(decDeg)
{
  try(if(decDeg < 0) stop("Degrees must be >= 0"))
  try(if(decDeg > 360) stop("Degrees must be <= 360"))

  decHr <- decDeg / 15

  return (decHr)
}

#' Convert hours to degrees
#' @description
#' Function to convert decimal hours to  decimal degrees. Hours should
#' always be positive as degrees must always be positive.
#' @param decHr Numeric
#' @return decimal degrees
#' @export
#' @examples
#' hrToDeg(10.42)
#' hrToDeg(6)
#' hrToDeg(12)
#' hrToDeg(18)
#' hrToDeg(-1)
#' hrToDeg(25)

HrToDeg <- function(decHr)
{
  try(if(decHr < 0) stop("Hours must be >= 0"))
  try(if(decHr > 24) stop("Hours must be <= 24"))

  decDeg <- decHr * 15

  return (decDeg)
}

#' Convert decimal hours to degrees, arc minutes, and arc seconds.
#' @description
#' Function to convert decimal hours to degrees, arc minutes, and arc seconds.
#' @param decHr Numeric
#' @return A vector of length 3
#' @export
#' @examples
#' hrToDMS(6)
#' hrToDMS(12)
#' hrToDMS(18)
#' hrToDMS(18.5)
#' hrToDMS(-5)
#' hrToDMS(25)

HrToDMS <- function(decHr)
{
  try(if(decHr < 0) stop("Hours must be >= 0"))
  try(if(decHr > 24) stop("Hours must be <= 24"))

  decDeg <- hrToDeg(decHr)

  return (degToDMS(decDeg))
}

#' Convert decimal degrees to hours, minutes, and seconds.
#' @description
#' Function to convert decimal degrees to hours, minutes, and seconds.
#' @param decDeg Numeric
#' @return A vector of length 3
#' @export
#' @examples
#' degToHMS(-5)
#' degToHMS(361)
#' degToHMS(90)
#' degToHMS(180)
#' degToHMS(270)
#' degToHMS(359)

DegToHMS <- function(decDeg)
{
  try(if(decDeg < 0) stop("Degrees must be >= 0"))
  try(if(decDeg > 360) stop("Degrees must be <= 360"))

  decHr <- degToHour(decDeg)

  return (hourToHMS(decHr))
}
