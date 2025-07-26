ColoradoPPDMLoader
Loading of Colorado oil & gas data into PPDM

The release have a self contained executable that you can download. This does not have a certificate so you will get a warning when using it.

It is using software from Selenium to download data. Selenium is basically test software for web based user interface. We are using their chrome driver for this.

Usage:
  ColoradoPPDMLoader [options]

Options:
  -c, --connection-string <connection-string> (REQUIRED)  The connection string for the database.
  --datatype <Deviations|Wellbore> (REQUIRED)             Data type to process: Wellbore or Deviations
  --version                                               Show version information
  -?, -h, --help                                          Show help and usage information
