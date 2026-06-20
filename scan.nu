def main [file_path: string, column_name: string] {
  let headers = open $file_path | lines | get 2 | from json
  let column_index = ($headers | enumerate | where {|x| $x.item == $column_name }) | first | get index

  print "["

  open $file_path | lines | enumerate | skip 3 | each { |x|
      let line  = $x.item
      let line_index  = $x.index

      let json = $line | from json
      let has_fin = not ($json | columns | where {|x| x == "fin"} | is-empty)
      let has_hist = not ($json | columns | where {|x| x == "histogram"}  | is-empty)

      if not $has_fin and not $has_hist {
        let time_ms = $json | first
        let value = $json | get $column_index

        if (($value | describe) == "int") {
            if $line_index > 3 {
                print ","
            }
            print --no-newline $"  [($time_ms), ($value)]"
        }
      }
  }

  print "\n]"
}
