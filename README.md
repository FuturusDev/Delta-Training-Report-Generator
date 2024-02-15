# Delta-Training-Report-Generator

Run `Delta Training Report Generator.exe` in `bin\debug\net6` to generate Delta Training Data.

The following values are available in the `App.config` to be updated:

  * `LocalPath` - The path that the CSV file will be written to on the system running the .exe
  * `FilePrepend` - The name at the front of each file (followed by date timestamp and either the ChrisId or "ALL")
  * `DayInterval` - The amount of days in the past that it pulls data from
  * ~~`ExcludeFuturusData` - A boolean flag that will exclude data from the Unity Editor and Development builds if set to `true` and will return all data if set to `false`~~ Not yet implemented
