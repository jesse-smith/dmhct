#' Common Patterns Representing Missing Data
#'
#' `na_patterns` is a collection of regular expression that commonly represent
#' missing data, especially when the character vector should be converted to
#' something else. These are designed to match strings that have already been
#' standardized.
#'
#' @export
na_patterns <- c(
  "^[^A-Z0-9]*$", # Includes ""
  "^REPORTED$",
  "\\b(?:C?QNS|IQ|N/?A|NG|NR|NSQ|NULL|QIS?)\\b",
  "\\b(?:NEVER DROPPED BELOW|SEEN? [A-Z]+|SYSTEM CLEAN|PREVIOUSLY CHARTED)\\b",
  "\\b(?:CANCEL|EQUIVOC|INADEQ|INCON|INSUFF|ONGOING|UNK|UNSPEC)",
  "\\bNOT? (?:AMP|APPL|AVAIL|DATA|DETER|DONE|EVAL|FOLLOWED|IDENT|VALUE)",
  "\\bNOT? (?:RER?PORT|(?:[A-Z ]|-)*PHENOTYPE)",
  "-999[0-9]"
)
