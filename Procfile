web: python -m http.server $PORT
parsenotices: python -m infomed_html_parser.cli s3 --pattern N --batch-size 1000
parsercp: python -m infomed_html_parser.cli s3 --pattern R --batch-size 1000
