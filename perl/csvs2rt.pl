#!/usr/bin/perl -w

use lib ".";
use lib "../epic";

use Datus;
use Date;
$Date::ref_date = new Date (2000, 1, 1);

use open qw(:std :utf8);

use Text::CSV_PP;
my $parser = Text::CSV_PP->new({binary => 1});

# if ($ARGV[0] =~ m/^\d+$/) {
#   $FID = $ARGV[0]
# } else {
#   die "First argument must be a numeric id of RT file to parse";
# }

# $fname = "CSV_FILES/RT${FID}.csv";

die "Last argument must be a readable file."
    unless (defined $ARGV[-1] and -r $ARGV[-1]);

$fname = $ARGV[-1];

# $FNAME_POST = '_100517';
$FNAME_POST = '';

($FID) = $fname =~ m!CSV_FILES/([\w\d_-]+)${FNAME_POST}\.csv!;

die "Filename does not match expected pattern"
    unless defined $FID;

$oname = "RT_FILES/${FID}.rtt";

# Field used to identify / index between files
$INDEX_FIELD = 0;
# First data field
$FIRST_DATA_FIELD = 1;

$STRUCT_NAME = $FID;

# If all fields start with FID prefix, this can remove it.
$remove_fid_from_field_names = 0;

# Try to convert unknowns to dates, ints or floats. Warnings will be printed to STDERR.
$convert_unknowns_to_something = 1;
# Fraction of dates and of ints/floats over all non-empty fields to convert to int/float.
# Default is rather greedy for dates as some of those might be like date of death.
# Otherwise converts to string.
$convert_unknowns_to_date_threshold   = 0.10;
$convert_unknowns_to_number_threshold = 0.75;

# Some fields can have <0.01 or >70000.
# By default, < and > are just stripped.
# Set the following to set < to 0 and > to 1.1 * max_column_value
$less_to_0_greater_to_110pct_of_max = 0;

$unknown_flt_value    = -3.0;
$unknown_int_value    = -3;
$undefed_flt_value    = -2.0;
$undefed_int_value    = -1;
$undefined_date_value = -1;

$SORT_IF_NEEDED = 0; # Needed for CSV file that should not be sorted

$DO_FLATTEN = ($FID =~ m/_TO_FLATTEN$/);
$FLATTEN_EQ_FIELD    =  6;   # Field that must be the same (+ id, of course)
$FLATTEN_EQ_RANGE    =  3;   # When > 0, allow this range of values; min is reported in standard value;
                             # varname_range is also set
$FLATTEN_EQ_XCHCEK   = -1; # Field that might have to be the same
$FLATTEN_NAME_FIELD  =  2;
$FLATTEN_VALUE_FIELD =  $FLATTEN_NAME_FIELD + 1;
$FLATTEN_UNIT_FIELD  =  $FLATTEN_NAME_FIELD + 2;
@FLATTEN_KEEP_FIELDS =  (0, 5, 6, 7);
$FLATTEN_KEEP_UNIT   =  0; # When set reference_unit field is stored for each entry
$FLATTEN_CHECK_UNIT  =  1; # When set it is checked that every entry for some name uses the same unit

$print_var_index = 0;
$print_fields    = 0;
$print_seqids    = 0;
$print_struct    = 1;
$make_rtt_file   = 1;
$make_root_tree  = 0;

################################################################################

$less_re = "<|<=";
$more_re = ">=|>";
$less_more_re = "${less_re}|${more_re}";

$LMYC = "(${less_more_re})?\\s*";   # less-more-yes-capture
$LMNC = "(?:${less_more_re})?\\s*"; # less-more-not-capture

$INTC = "([-+]?\\d+)";
$FLTC = "([-+]?(?:\\d*\\.\\d*(?:[eE][-+]?\\d+)?)|\\d+[eE][-+]?\\d+)";

$undef_re = "(?:invalid|inval|duplicate|null|pending)";

################################################################################

# Parse assign and closure arguments, if any: var=val or '{<closure>}'

while (1)
{
  if ($ARGV[0] =~ m/^(\w+)=(-?\d+)$/)
  {
    print STDERR "eval \$$1=$2\n" unless $quiet;
    eval "\$$1=$2";
    shift @ARGV;
  }
  elsif ($ARGV[0] =~ m/^\{.*\}$/)
  {
    print STDERR "eval $ARGV[0]\n" unless $quiet;
    eval $ARGV[0];
    shift @ARGV;
  }
  else
  {
    last;
  }
}

################################################################################

$lineno = 0;

sub next_line
{
  while (my $l = <L>)
  {
    ++$lineno;

    next if $l =~ m/^#/;

    #chomp $l;
    $l =~ s/\r?\n?$//;
    
    return $l;
  }

  return undef;
}

sub reset_file_pos
{
  seek L, 0, 0;
  $lineno = 0;
}

################################################################################

open L, $fname or die "Can not open $fname";

reset_file_pos();

$l1 = next_line();

## print $l1;

my $ps = $parser->parse($l1);

## print "Parse status $ps\n";

# Full fields
my @ff = $parser->fields();

## print join("\n", @ff), "\n";

# Fields with removed "legends"
my @f = $parser->fields();
s/\s*\(.*//og for @f;
s/\xab/p/og   for @f;

# modify field names to be valid variable names
for $f (@f)
{
  $f =~ s/[^[:ascii:]]//og;
  $f =~ s/^(?=\d)/n/o;
  $f =~ s!(?:-| |/|\?|=)!_!og;
  $f =~ s/%/pct/og;
  $f =~ s/_+$//o;

  if ($remove_fid_from_field_names)
  {
    $f =~ s/${FID}$//o;
  }
}

# print variable index

$num_fields = @f;

if ($print_var_index)
{
  print "*** BEGIN VAR INDEX - Num fields $num_fields\n";
  for ($i = 0; $i < $num_fields; ++$i)
  {
    printf "%3d | %-45s | %s\n", $i, $f[$i], $ff[$i];
  }
  print "* END VAR INDEX\n\n";
}

########################################################################

@database = ();
@index    = ();

@dat = @tim = @flt = @itg = @undefed = @non_empty = (0) x $num_fields;
@cmin = (+9999999) x $num_fields;
@cmax = (-9999999) x $num_fields;

%MNAME2INT = (
  'Jan' =>  1, 'Feb' =>  2, 'Mar' =>  3, 'Apr' =>  4, 'May' =>  5, 'Jun' =>  6,
  'Jul' =>  7, 'Aug' =>  8, 'Sep' =>  9, 'Oct' => 10, 'Nov' => 11, 'Dec' => 12
);

$DATE_MNAME_RE = "(\\d+)-(" . join("|", keys %MNAME2INT) . ")-(\\d+)";

while (my $l = next_line())
{
  $parser->parse($l);
  my @es = $parser->fields();
  my $nes = @es;

  # Assert num == num_fields
  if ($nes != $num_fields)
  {
    die "Line $lineno: nes=$nes != nf=$num_fields\n";
    # print "Line $lineno: nes=$nes != nf=$num_fields\n";
    # next;
  }

  # Remove front/back whitespace.
  @es = map { s/^\s+//; s/\s+$//; $_; } @es;

  $database[$lineno] = [ @es ];

  push @index, [$es[$INDEX_FIELD], $lineno];

  my $col = 0;
  for my $e (@es)
  {
    ## print "$lineno:$col = '${e}'\n" if $col==5;

    $e = "" if $e =~ m!^\s*null\s*$!i;
    
    if ($e =~ m!^(\d+)/(\d+)/(\d+)$! or $e =~ m!^(\d+)-(\d+)-(\d+)$! or $e =~ m!${DATE_MNAME_RE}!)
    {
      # date
      ++$dat[$col];

      # printf "  %-10s  %f\n", $e , Datus::date_to_double($e);
    }
    if ($e =~ m!^(\d+):(\d+):(\d+)$!)
    {
      # time
      ++$tim[$col];
    }
    if ($e =~ m!^${LMNC}${FLTC}$!)
    {
      # float
      ++$flt[$col];
      $cmin[$col] = $1 if $1 < $cmin[$col];
      $cmax[$col] = $1 if $1 > $cmax[$col];
    }
    if ($e =~ m!^${LMNC}${INTC}$!)
    {
      # int
      ++$itg[$col];
      $cmin[$col] = $1 if $1 < $cmin[$col];
      $cmax[$col] = $1 if $1 > $cmax[$col];
    }
    if ($e =~ m!^${undef_re}$!i)
    {
      # if ($col == 13) { print "At $lineno:$col '$e' matched to '$undef_re'.\n"; }
      ++$undefed[$col];
    }
    if ($e =~ m!\S+! and not $e =~ m!null!i)
    {
      # non-empty
      ++$non_empty[$col];
    }

    ++$col;
  }
}

use sort 'stable';

my $needs_sort = 0;

for (my $i = 0; $i < $#index; ++$i)
{
  if ($index[$i][0] > $index[$i + 1][0])
  {
    $needs_sort = "at line $i index is $index[$i][0], on next line it is " . $index[$i + 1][0];
    last;
  }
}

if ($needs_sort)
{
  if ($SORT_IF_NEEDED) {
    print STDERR "Index sorting required\n";
    @index = sort { $a->[0] <=> $b->[0] } @index;
  }
  else {
    print STDERR "Index sorting would be required but is DISABLED\n";
  }
}
else
{
  print STDERR "Index sorting not required.\n";
}

$n_index = @index;

$entries_sparse = 0;
$entries_clones = 0;

$max_entries_per_id = 0;
$cur_entries_per_id = 0;

{
  my $prev_seqnum = $index[0]->[$INDEX_FIELD];
  $cur_entries_per_id = 1;

  for (my $i = 1; $i < $n_index; ++$i)
  {
    my $seqnum = $index[$i]->[$INDEX_FIELD];

    if ($seqnum == $prev_seqnum)
    {
      $entries_clones = 1;
      ++$cur_entries_per_id;
    }
    else
    {
      if ($seqnum > $prev_seqnum + 1)
      {
        $entries_sparse = 1;
      }

      $max_entries_per_id = $cur_entries_per_id if $cur_entries_per_id > $max_entries_per_id;
      $cur_entries_per_id = 1;
      $prev_seqnum = $seqnum;
    }
  }

  $max_entries_per_id = $cur_entries_per_id if $cur_entries_per_id > $max_entries_per_id;
}

if ($print_seqids)
{
  for (my $i = 0; $i < $n_index; ++$i)
  {
    print $index[$i]->[$INDEX_FIELD], "\n";
  }
  print STDERR "Exiting after printing sequids, entries_clones=$entries_clones, entries_sparse=$entries_sparse.\n";
  exit 0;
}

print "*** BEGIN FIELDS\n"  if ($print_fields);

for (my $i = 0; $i < $num_fields; ++$i)
{
  my $num_cnt = $itg[$i] + $flt[$i];

  if ($non_empty[$i] < 1)
  {
    $type[$i] = "Unknown";
    $ctyp[$i] = "int";
    $frmt[$i] = "I";
    $dflt[$i] = "-9999";
  }
  elsif ($dat[$i] == $non_empty[$i])
  {
    $type[$i] = "Date";
    $ctyp[$i] = "int"; # "double";
    $ccom[$i] = "time in days since " . $Date::ref_date->prdashcompact(); # "date in years";
    $frmt[$i] = "I"; # "D";
    $dflt[$i] = "-9999";
  }
  elsif ($tim[$i] == $non_empty[$i])
  {
    $type[$i] = "Time";
    $ctyp[$i] = "double";
    $ccom[$i] = "time or duration in hours";
    $frmt[$i] = "D";
    $dflt[$i] = "-9999";
  }
  elsif ($itg[$i] + $undefed[$i] == $non_empty[$i])
  {
    $type[$i] = "Int";
    $ctyp[$i] = "int";
    $frmt[$i] = "I";
    $dflt[$i] = "-9999";
  }
  elsif ($itg[$i] + $flt[$i] + $undefed[$i] == $non_empty[$i])
  {
    $type[$i] = "Float";
    $ctyp[$i] = "double";
    $frmt[$i] = "D";
    $dflt[$i] = "-9999";
  }
  elsif ($non_empty[$i] > 0 && (($num_cnt > 0 && $num_cnt < $non_empty[$i]) || ($dat[$i] > 0 && $dat[$i] < $non_empty[$i]) || ($tim[$i] > 0 && $tim[$i] < $non_empty[$i])) )
  {
    $type[$i] = "XXXXX";
    $ctyp[$i] = "unclear_probably_entry_error";
    $frmt[$i] = "C";
    $dflt[$i] = "\"XXX\"";
  }
  else
  {
    $type[$i] = "String";
    $ctyp[$i] = "TString"; # "std::string";
    $frmt[$i] = "C";
    $dflt[$i] = "\"XXX\"";
  }

  if ($type[$i] eq "XXXXX" and $convert_unknowns_to_something)
  {
    my $date_frac = $dat[$i] / $non_empty[$i];
    my $num_frac  = $num_cnt / $non_empty[$i];

    if ($date_frac > $convert_unknowns_to_date_threshold)
    {
        $type[$i] = "Date";
        $ctyp[$i] = "int"; # "double";
        $ccom[$i] = "time in days since " . $Date::ref_date->prdashcompact() . "; converted from unknown with $date_frac > $convert_unknowns_to_date_threshold";
        $frmt[$i] = "I"; # "D";
        $dflt[$i] = "-9999";
    }  
    elsif ($num_frac >  $convert_unknowns_to_number_threshold)
    {
      if ($flt[$i] > 0)
      {
        $type[$i] = "Float";
        $ctyp[$i] = "double";
        $frmt[$i] = "D";
        $dflt[$i] = "-9999";
        $ccom[$i] = "converted from unknown with $num_frac > $convert_unknowns_to_number_threshold";
      }
      else
      {
        $type[$i] = "Int";
        $ctyp[$i] = "int";
        $frmt[$i] = "I";
        $dflt[$i] = "-9999";
        $ccom[$i] = "converted from unknown with $num_frac > $convert_unknowns_to_number_threshold";
      }
    }
    else
    {
      $type[$i] = "String";
      $ctyp[$i] = "TString"; # "std::string";
      $frmt[$i] = "C";
      $dflt[$i] = "\"XXX\"";
      $ccom[$i] = "converted from unknown with date_frac=$date_frac and num_frac=$num_frac (thresholds are $convert_unknowns_to_date_threshold and $convert_unknowns_to_number_threshold)";
    }
  }

  # HACK to fix date field names to something reasonable. Used in epic-v6, for some reason.
  # my $done_date_fix = 0;
  # # -- something else happens ...
  # if ($f[$i] =~ m/_date2$/)
  # {
  #   if ($done_date_fix)
  #   {
  #     $f[$i]=~ s/2$//;
  #   }
  #   else
  #   {
  #     $f[$i] = "Date";
  #     $done_date_fix = 1;
  #   }
  # }

  $namefmt[$i] = "$f[$i]/$frmt[$i]";

  if ($print_fields)
  {
    printf "%3d | %-45s | %-8s | %s [%d,%d,%d,%d ; %d ; %d]\n", $i, $f[$i], $type[$i], $ff[$i],
        $dat[$i], $tim[$i], $flt[$i], $itg[$i], $undefed[$i], $non_empty[$i];

    printf "    min = %d, max = %d\n", $cmin[$i], $cmax[$i] if $type[$i] eq "Int";
    printf "    min = %f, max = %f\n", $cmin[$i], $cmax[$i] if $type[$i] eq "Float";
    if ($type[$i] eq "XXXXX") ### || $type[$i] eq "String")
    {
      printf "    Date: %2d, Float: %2d, Int: %2d, Non-empty: %2d\n", $dat[$i], $flt[$i], $itg[$i], $non_empty[$i];
    }
  }
}
print "* END FIELDS\n\n"  if ($print_fields);


#-----------------------------------------------------------------------
# BEGIN Flattening for files that end with _TO_FLATTEN
#-----------------------------------------------------------------------

@BACK_PAINS = qw( UCI LABCORP QUEST EXTERNAL );

sub norm_field_name
{
  my $txt = shift;
  for $bp (@BACK_PAINS) { $txt =~ s/${bp}$//; }
  $txt =~ s!%!PCT!og;
  $txt =~ s![- /,()\.]+!_!og;
  $txt =~ s/_+$//o;
  return $txt;
}

sub norm_unit_name
{
  my $txt = shift;

  $txt =~ s!mm Hg!mmHg!og;
  $txt =~ s!mmHg \+ CVP!mmHg!og;

  return $txt;
}

# --------------------------------

sub write_ecg_line
{
  my $vals    = shift;
  my $fields  = shift;
  my $val     = shift;
  my $val_max = shift;

  if ($FLATTEN_EQ_RANGE)
  {
    $vals->{$f[$FLATTEN_EQ_FIELD]}            = $val;
    $vals->{$f[$FLATTEN_EQ_FIELD] . "_range"} = $val_max - $val;
  }

  my @out = map { defined $vals->{$_} ? $vals->{$_} : "" } @$fields;

  $parser->print(\*FLTNOUT, \@out);  print FLTNOUT "\n";
}

# --------------------------------

if ($DO_FLATTEN)
{
  my $ofname = $fname;
  $ofname =~ s/_TO_FLATTEN//;

  open FLTNOUT, ">$ofname";

  print "FLATTEN requested - output file $ofname.\n";

  # Collect all sub-fields, normalize field names

  for my $dbi (@index)
  {
    $lineno = $dbi->[1];
    my @es = @{$database[$lineno]};

    ++$subcnt{$es[$FLATTEN_NAME_FIELD]};
    ++$normcnt{norm_field_name($es[$FLATTEN_NAME_FIELD])};
  }

  my @ecg_fields = map { $f[$_] } @FLATTEN_KEEP_FIELDS;

  if ($FLATTEN_EQ_RANGE)
  {
    push @ecg_fields, $f[$FLATTEN_EQ_FIELD] . "_range";
  }

  # Determine which measurement fields to store, add them to ecg_fields
  {
    my %fields_stored;

    for my $k (sort keys %subcnt)
    {
      my $name = norm_field_name($k);

      ### XXXX 100 count hardcoded XXXX ###
      my $keep = $normcnt{$name} >= 100;

      printf "%s %-30s present %5d times - normalized name %-30s (present %5d times).\n",
          $keep ? "Keep" : "Drop", $k, $subcnt{$k}, $name, $normcnt{$name};

      if ($keep and not exists $fields_stored{$name})
      {
        push @ecg_fields, $name;
        push @ecg_fields, $name . "_unit" if $FLATTEN_KEEP_UNIT;
        $fields_stored{$name} = 1;
      }
    }
    printf "\n";
  }

  my %unit_valid; # The accepted / most likely unit for given measurement

  if ($FLATTEN_CHECK_UNIT)
  {
    my $unit_check = {}; # Unit for first entry of a given name; used for $FLATTEN_CHECK_UNIT

    for my $dbi (@index)
    {
      $lineno = $dbi->[1];

      my @es = @{$database[$lineno]};

      my $nn = norm_field_name($es[$FLATTEN_NAME_FIELD]);

      ++$unit_check->{$nn}{'count'};
      ++$unit_check->{$nn}{'units'}{$es[$FLATTEN_UNIT_FIELD]};
    }

    for my $k (sort keys %{$unit_check})
    {
      my $hr = $unit_check->{$k};
      my $ur = $hr->{'units'};

      my @sorted_units = sort { $ur->{$b} <=> $ur->{$a} } keys %{$ur};

      print "Measurement $k: N_entries = ", $hr->{'count'} , ", N_units = ", scalar keys %{$ur},
          "; unit with highest number of entries = ", $sorted_units[0], ", count = ", $ur->{$sorted_units[0]},
          "\n";
      for my $u (@sorted_units)
      {
        printf "   %8s   %s  --> Normalized name %s\n", $u,  $ur->{$u}, norm_unit_name($u);
      }

      $unit_valid{$k} = norm_unit_name( $sorted_units[0] );
    }
    print "\n";
  }

  $parser->print(\*FLTNOUT, \@ecg_fields);  print FLTNOUT "\n";

  my %vals;
  my $cnt;
  my $idx;
  my $prev_idx     = -1;
  my $prev_val     = -9999;
  my $prev_val_max = -9999;

  my $prev_xcheck_val = "";

  for my $dbi (@index)
  {
    $lineno = $dbi->[1];

    my @es = @{$database[$lineno]};

    my $idx = $es[$INDEX_FIELD];
    my $val = $es[$FLATTEN_EQ_FIELD];

    my $in_eq_range = 0;

    if ($idx == $prev_idx)
    {
      $in_eq_range = ($val == $prev_val);

      if ($FLATTEN_EQ_RANGE)
      {
        if ($val < $prev_val)
        {
          if ($prev_val_max - $val <= $FLATTEN_EQ_RANGE) { $prev_val = $val; $in_eq_range = 1; }
        }
        elsif ($val > $prev_val_max)
        {
          if ($val - $prev_val <= $FLATTEN_EQ_RANGE) { $prev_val_max = $val; $in_eq_range = 1; }
        }
        else # equal
        {
          $in_eq_range = 1;
        }
      }

      if ($FLATTEN_EQ_XCHCEK > -1)
      {
        if ($in_eq_range)
        {
          print "EQ_XCHECK consistency error code 1 at line $lineno\n" if ($es[$FLATTEN_EQ_XCHCEK] ne $prev_xcheck_val);
        }
        else
        {
          print "EQ_XCHECK consistency error code 2 at line $lineno\n" if ($es[$FLATTEN_EQ_XCHCEK] eq $prev_xcheck_val);
        }
      }
    }

    if ($idx != $prev_idx or not $in_eq_range)
    {
      if ($prev_idx != -1)
      {
        write_ecg_line(\%vals, \@ecg_fields, $prev_val, $prev_val_max);
      }
      %vals = map { $f[$_] => $es[$_] } @FLATTEN_KEEP_FIELDS;
      $prev_idx = $idx;
      $prev_val = $prev_val_max = $val;
    }

    my $nn = norm_field_name($es[$FLATTEN_NAME_FIELD]);
    my $nu = norm_unit_name ($es[$FLATTEN_UNIT_FIELD]);

    if (not $FLATTEN_CHECK_UNIT or ($FLATTEN_CHECK_UNIT and $nu eq $unit_valid{$nn}))
    {
      $vals{$nn}           = $es[$FLATTEN_VALUE_FIELD];
      $vals{$nn . "_unit"} = $nu if $FLATTEN_KEEP_UNIT;
    }

    $prev_xcheck_val = $es[$FLATTEN_EQ_XCHCEK]; # Store EQ_XCHECK value
  }
  write_ecg_line(\%vals, \@ecg_fields, $prev_val, $prev_val_max);

  print "FLATTEN finished, exiting.\n\n";
  close FLTNOUT;
  exit 0;
}

#-----------------------------------------------------------------------
# END of flattening
#-----------------------------------------------------------------------


if ($print_struct)
{
  print "// First seq $index[0]->[0], last seq $index[-1]->[0], n_lines = $n_index\n";
  print "// n_fields=$num_fields, entries_sparse=$entries_sparse, entries_clones=$entries_clones\n";
  print "struct $STRUCT_NAME\n{\n";
  print "  static const int s_max_ent = $max_entries_per_id;\n\n";

  for (my $i = $FIRST_DATA_FIELD; $i < $num_fields; ++$i)
  {
    printf "  %-15s %s = %s;%s\n", $ctyp[$i], $f[$i], $dflt[$i], defined $ccom[$i] ? " // $ccom[$i]" : "";
  }

  print "\n";

  print <<'END';
  int ReadLine(FILE *fp, TPMERegexp &splitter)
  {
    TString l;
    if ( ! l.Gets(fp)) return -1;
    splitter.Split(l);
END
  for (my $i = $FIRST_DATA_FIELD; $i < $num_fields; ++$i)
  {
    if ($ctyp[$i] eq "TString") # "std::string")
    {
      print "    $f[$i] = splitter[$i];\n"; # .Data();\n";
    }
    else
    {
      my $foo;
      if    ($ctyp[$i] eq "int")    { $foo = "Atoi"; }
      elsif ($ctyp[$i] eq "double") { $foo = "Atof"; }
      else  { die "Unsupported c-type $ctyp[$i]"; }

      print "    $f[$i] = splitter[$i].${foo}();\n";
    }
  }
  print "    return splitter[$INDEX_FIELD].Atoi();\n";
  print "  }\n";

  print "};\n\n";
}

################################################################################

sub norm_int
{
  my $txt = shift;

  if ($txt =~ m!^${LMYC}${INTC}$!)
  {
    if (defined $1)
    {
      if ($less_to_0_greater_to_110pct_of_max)
      {
        my $lm = $1;
        if    ($lm =~ m!${less_re}!) { $txt = 0; }
        elsif ($lm =~ m!${more_re}!) { $txt = int(1.1 * $cmax[$columnno]); }
        else  { die "String $lm is neither less nor more."; }
      }
      else
      {
        $txt = $2;
      }
    }
  }
  else
  {
    if (not defined $txt or $txt eq "" or $txt =~ m!${undef_re}!i)
    {
      $txt = $undefed_int_value;
    }
    else
    {
      print STDERR "$fname:$lineno/$columnno - bad int: '$txt'\n";
      $txt = $unknown_int_value;
    }
  }

  return $txt;
}

sub norm_flt
{
  my $txt = shift;

  if ($txt =~ m!^${LMYC}${FLTC}$! or $txt =~ m!^${LMYC}${INTC}$!)
  {
    if (defined $1)
    {
      if ($less_to_0_greater_to_110pct_of_max)
      {
        my $lm = $1;
        if    ($lm =~ m!${less_re}!) { $txt = 0.0; }
        elsif ($lm =~ m!${more_re}!) { $txt = int(1100 * $cmax[$columnno])/1000; }
        else  { die "String $lm is neither less nor more."; }
      }
      else
      {
        $txt = $2;
      }
    }
  }
  else
  {
    if (not defined $txt or $txt eq "" or $txt =~ m!${undef_re}!i)
    {
      $txt = $undefed_flt_value;
    }
    else
    {
      print STDERR "$fname:$lineno/$columnno - bad flt: '$txt'\n";
      $txt = $unknown_flt_value;
    }
  }

  return $txt;
}

sub norm_str
{
  my $txt = shift;

  $txt = " " unless length $txt;

  return $txt;
}

################################################################################

if ($make_rtt_file)
{
  open O, ">$oname" or die "Can not open $oname for writing";

  print O join("|", @namefmt), "\n";

  for my $dbi (@index)
  {
    $lineno = $dbi->[1];

    my @es = @{$database[$lineno]};

    my @oes;

    for ($i = 0; $i < $num_fields; ++$i)
    {
      $columnno = $i;

      my $e = $es[$i];
      my $t = $type[$i];

      if    ($t eq "Date")  { push @oes, Date::days_from_reference($e, $undefined_date_value); }
      elsif ($t eq "Time")  { push @oes, Datus::time_to_double($e); }
      elsif ($t eq "Int")   { push @oes, sprintf("%d", norm_int($e)); }
      elsif ($t eq "Float") { push @oes, sprintf("%f", norm_flt($e)); }
      else                  { push @oes, norm_str($e); }
    }

    print O join("|", @oes), "\n";
  }

  close O;

  print STDERR Date::report_min_max_days(), "\n";
}

################################################################################

if ($make_root_tree)
{
  open RRR, "|root.exe";
  print RRR <<"FNORD";
 TTree t("t", "${fname}");
 t.ReadFile("RT${FID}.rtt", "", '|');
 f=TFile::Open("RT${FID}.root", "RECREATE");
 t.Write();
 f->Close();
 .q
FNORD
  close RRR;
}
