# Parser Notes

The real ClusterDelta stream should be normalized into two arrays:
- tape rows
- DOM rows

The MQL bridge can post JSON payloads to the Python server, which then maps them into the existing ingestion pipeline.
