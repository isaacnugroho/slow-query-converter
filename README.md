# MariaDB Slow Query to CSV Converter

This program parses a MariaDB slow query log file and converts its contents into a CSV file where each query and its associated metadata are on a single row.
The 'query' field is multiline-aware and quoted, making it suitable for direct
import into spreadsheet software like Microsoft Excel. It handles optional '# Time' headers by carrying forward the last seen time value.
