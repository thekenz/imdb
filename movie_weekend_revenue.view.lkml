#
# Boxoffice data is stored as type 107.  The records we are interested in look like:
#  $99,999 (USA) 10 November 2012
#
# There is a number for each weekend.
# Currently working on derived table here: https://exploreday.looker.com/sql/svyytjytskgnzf
view: movie_weekend_revenue {
  derived_table: {
    persist_for: "100 hours"
    indexes: ["movie_id"]
    sql: SELECT
        *
        , ROW_NUMBER() OVER(ORDER BY movie_id, weekend_date) as id
        , ROW_NUMBER() OVER(PARTITION BY movie_id ORDER BY weekend_date) as weekend_number
        , amount_to_date - COALESCE(LAG(amount_to_date) OVER (PARTITION BY movie_id ORDER BY weekend_date),0) as weekend_amount
        , MAX(amount_to_date) OVER(PARTITION BY movie_id) as max_revenue
      FROM (
        SELECT
          movie_id
          , info as info
          , CAST(REPLACE(REPLACE(REGEXP_EXTRACT(info, r"([^\s]+)"), "$", ""), ",", "") AS FLOAT64) / 1000000.0
            as amount_to_date -- this is working
          , DATE(RTRIM(REGEXP_EXTRACT(info,'[^\\(]*$'),")"),'DD Month YYYY') as weekend_date -- this is not
        FROM `lookerdata.imdb.movie_info` as movie_info
        WHERE
          movie_info.info_type_id = 107
          AND
              info ILIKE '$%(USA)%(%)' and info ~ '\\d\\d [A-Z][a-z]* \\d\\d\\d\\d\\\)$'


      ) AS BOO
       ;;
  }

  dimension: id {
    hidden: yes
    primary_key: yes
  }

  dimension: movie_id {
    hidden: yes
  }

  dimension: weekend_amount {
    type: number
    value_format: "$#,##0.00 \M"
  }

  dimension: info {}

  dimension_group: weekend {
    type: time
    timeframes: [date]
    sql: ${TABLE}.weekend_date ;;
  }

  dimension: weekend_number {
    type: number
  }

  dimension: max_revenue {
    type: number
  }

  measure: total_amount {
    type: sum_distinct
    sql: ${max_revenue} ;;
    sql_distinct_key: ${movie_id} ;;
    value_format: "$#,##0.00 \M"
  }

  measure: average_amount {
    type: average
    sql: ${weekend_amount} ;;
    value_format: "$#,##0.00 \M"
  }
}
