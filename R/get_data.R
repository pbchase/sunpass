#' Read and clean Sunpass activity files
#'
#' By default, ~/Downloads folder
#'
#' @param input_dir the directory in which activity file can be found
#' @param pattern the file name pattern for Sunpass Activity files
#'
#' @return a cleaned dataframe of every distinct transaction in every
#'     file in input_dir that matches the pattern
#' @export
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @examples
#' \dontrun{
#' get_activity()
#' }
get_activity <- function(input_dir = "~/Downloads",
                         pattern = "Transactions_transaction_csv_report_") {
  sunpass_csv_files <- list.files(path = input_dir, pattern, full.names = T)
  result <-
    sunpass_csv_files %>%
    purrr::map_dfr(readr::read_csv) %>%
    janitor::clean_names() %>%
    dplyr::distinct(.data$transaction_number, .keep_all = T) %>%
    dplyr::mutate(
      debit = as.numeric(stringr::str_replace(.data$debit, "[$]", "")),
      credit = as.numeric(stringr::str_replace(.data$credit, "[$]", ""))
    ) %>%
    dplyr::mutate(
      transaction_dts = lubridate::mdy_hms(paste(.data$transaction_date, .data$transaction_time)),
      posted_date = lubridate::mdy(.data$posted_date),
      transaction_date = lubridate::mdy(.data$transaction_date)
    ) %>%
    dplyr::select(-.data$transaction_time)
  return(result)
}

#' Read and clean Sunpass transponder files
#'
#' By default, ~/Downloads folder
#'
#' @param input_dir the directory in which transponder file can be found
#' @param pattern the file name pattern for Sunpass transponder files
#'
#' @return a cleaned dataframe of every distinct transponder in every
#'     file in input_dir that matches the pattern
#' @export
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @examples
#' \dontrun{
#' get_transponder()
#' }
get_transponder <- function(input_dir = "~/Downloads",
                            pattern = "my_transponder_csv_report_") {
  sunpass_csv_files <- list.files(path = input_dir, pattern, full.names = T)
  result <- sunpass_csv_files %>%
    purrr::map_dfr(readr::read_csv) %>%
    janitor::clean_names() %>%
    dplyr::distinct(.data$transponder_number, .keep_all = T) %>%
    dplyr::mutate(associated_plate = dplyr::if_else(
      is.na(.data$associated_plate),
      .data$friendly_name,
      .data$associated_plate
    ))
  return(result)
}

#' Get a dataframe of all Sunpass data
#'
#' Read and join activity data to transponder data
#'
#' @return a dataframe of all sunpass data
#' @export
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @examples
#' \dontrun{
#' get_data()
#' }
get_data <- function() {
  result <- get_activity() %>%
    dplyr::left_join(get_transponder(), by = c("transponder_license_plate" = "transponder_number")) %>%
    dplyr::mutate(associated_plate = dplyr::if_else(
      is.na(.data$associated_plate),
      .data$transponder_license_plate,
      .data$associated_plate
    ))
  return(result)
}

#' get Sunpass data with an estimated trip_start_date
#'
#' Assign a new trip start date for every time a transponder has reported
#'   no activity for _gap_between_trips_ days.
#'
#' @param gap_between_trips the minimum number of days required to separate
#'     sequential transactions on a transponder into separate trips
#'
#' @return a data frame of Sunpass transactions with an additional trip_start_date column
#' @export
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @examples
#' \dontrun{
#' get_data_with_trip_start_date()
#' get_data_with_trip_start_date(gap_between_trips = 5)
#' }
get_data_with_trip_start_date <- function(gap_between_trips = 5) {
  result <- get_data() %>%
    dplyr::arrange(.data$transaction_dts) %>%
    dplyr::group_by(.data$associated_plate) %>%
    dplyr::mutate(previous_trans_dts = dplyr::lag(.data$transaction_dts)) %>%
    dplyr::mutate(
      trip_start_date =
        dplyr::if_else(
          .data$transaction_dts - .data$previous_trans_dts > lubridate::ddays(gap_between_trips) | is.na(.data$previous_trans_dts),
          .data$transaction_date,
          as.Date(NA)
        )
    ) %>%
    tidyr::fill(.data$trip_start_date) %>%
    dplyr::select(-.data$previous_trans_dts) %>%
    dplyr::ungroup()

  return(result)
}

#' Get cost for each trip in Sunpass data
#'
#' @param gap_between_trips the minimum number of days required to separate
#'     sequential transactions on a transponder into separate trips
#'
#' @return a data frame of trips with cost, start and end dates
#' @export
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @examples
#' \dontrun{
#' get_cost_by_trip()
#' get_cost_by_trip(gap_between_trips = 5)
#' }
get_cost_by_trip <- function(gap_between_trips = 5) {
  result <- get_data_with_trip_start_date(gap_between_trips = gap_between_trips) %>%
    dplyr::filter(.data$description_plaza_name != "PAYMENT & ADJUSTMENTS") %>%
    dplyr::group_by(.data$associated_plate, .data$trip_start_date) %>%
    dplyr::summarise(cost = sum(.data$debit)) %>%
    dplyr::arrange(.data$trip_start_date)

  return(result)
}

#' Get total costs incurred by each transponder
#'
#' @return a data from of cost incurred by each transponder
#' @export
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @examples
#' \dontrun{
#' get_cost_by_transponder()
#' }
get_cost_by_transponder <- function() {
  result <- get_data() %>%
    dplyr::filter(.data$description_plaza_name != "PAYMENT & ADJUSTMENTS") %>%
    dplyr::group_by(.data$associated_plate) %>%
    dplyr::summarise(
      cost = sum(.data$debit),
      start_date = min(.data$transaction_date),
      end_date = max(.data$transaction_date)
    )

  return(result)
}
