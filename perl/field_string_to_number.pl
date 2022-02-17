#!/usr/bin/perl -w

use open qw(:std :utf8);
use File::Copy;
use Text::CSV_PP;

my $parser = Text::CSV_PP->new({binary => 1});

# Print header
$no_header = 0;

# Do no print progress info to STDERR
$quiet = 0;

# Replace string entries in given column by a unique integer
undef $fidx;
$ftoupper   = 0; # Set to one to convert to uppercase.
$ffirstword = 0; # Set to one to only take first word - to first space character.

# Collapse several lines when COLLAPSE_EQ fields are equal, merge COLLAPSE_FIELD
# as string.
# NOTE - does numeric equality in COLLAPSE_EQ fields.
undef $cidx;
@COLLAPSE_EQ   = (0, 2);
$COLLAPSE_NO   = scalar @COLLAPSE_EQ; # recomputed after eval of args
$COLLAPSE_SEP  = ":";
$COLLAPSE_SORT = 0;
$COLLAPSE_UNIQ = 0; # Remove multiple identical entries

# Custom mapping function
undef $midx;

if ($#ARGV < 0)
{
  print STDERR<<"FNORD";
Usage: $0 show|test|do [fidx=n] [cidx=n] [midx=n] [sidx=n] '{<closure>}' csv-files-to-process
 Action selection:
  show    - run through all events and print some info
  debug   - run over first 100 entries and, probably, print some more info
  do      - run, process and replace the input CSV with modified lines
 Args:
  fidx    - field index to convert from string to integer
  cidx    - collapse index -- entries from several lines will be merged
  midx    - map index -- map values using custom functions / regexps
  sidx    - sort index -- sort entries within same patient-id (element 0) on this field
FNORD
  exit 1;
}

@act_av = qw( show test do );

$act = shift @ARGV;

die "action (first arg) must be '" . join("' or '", @act_av) . "'"
    unless grep { $act eq $_ } @act_av;

print "\n", join(" ", "- Running:", $0, $act, @ARGV), "\n" unless $no_header;


################################################################################
#
# sub setup_for_comorb_mapping
#
#

sub setup_for_comorb_mapping
{
  $MODIFY_LINE = sub
  {
    my $lar = shift;     # line-array-ref
    my $v = \$lar->[$midx];
    my $ov = $$v;
     my $e = '(?:\.|\\s|$)';

    if    ($$v =~ m/^I25${e}/) { $$v = 1; }
    elsif ($$v =~ m/^N18${e}/) { $$v = 2; }
    elsif ($$v =~ m/^I12${e}/) { $$v = 3; }
    elsif ($$v =~ m/^E11${e}/) { $$v = 4.1; }
    elsif ($$v =~ m/^E10${e}/) { $$v = 4.2; }
    elsif ($$v =~ m/^E08${e}/) { $$v = 4.3; }
    elsif ($$v =~ m/^E09${e}/) { $$v = 4.4; }
    elsif ($$v =~ m/^O24\.4/)  { $$v = 4.5; } 
    elsif ($$v =~ m/^E13${e}/) { $$v = 4.6; }
    elsif ($$v =~ m/^I10${e}/) { $$v = 5; }
    elsif ($$v =~ m/^J44${e}/) { $$v = 6; }
    elsif ($$v =~ m/^G47\.3/)  { $$v = 7; } 
    elsif ($$v =~ m/^I63${e}/) { $$v = 8.1; }
    elsif ($$v =~ m/^G45${e}/) { $$v = 8.2; }
    else {
      print "Error matching $$v\n";
      $$v = 9999;
    }
    # print "Mapping $ov to $$v\n";
  }
}

################################################################################
#
# sub setup_for_hosp_collapse
#
# Custom function for collapsing multiple diagnoses and counting number
# of HF-related yes / no tags on each of them.
# This must happen at the same time Ns and Ys are str2int-ed to 0s and 1s, e.g.:
#   ./field_string_to_number.pl ${S2I_ACT} fidx=7 cidx=6 '{add_known("N","Y");}' \
#		'{setup_for_hosp_collapse();}' CSV_FILES/hosp.csv >> ${S2I_REP}
# Goes with sample reAdmissions/20191004

sub setup_for_hosp_collapse
{
  if (($known[0] ne 'N' and $known[0] ne '0') or
      ($known[1] ne 'Y' and $known[1] ne '1'))
  {
    die "This is most likely not going to work out right ... inspect and fix";
  }

  $COLLAPSE_L1_FIX = sub
  {
    # Add N / Y count columns

    my $l1r = shift;
    $$l1r .= ",HF_N_CNT,HF_Y_CNT";
  };

  $COLLAPSE_NEW_LINE = sub
  {
    # Create entries for the extra fields

    my $lar = shift;         # line-array-ref
    my $v  = $lar->[$fidx];  # initial value (after str2int)
    push @$lar, $v == 0 ? 1 : 0, $v == 1 ? 1 : 0;
  };

  $COLLAPSE_NEXT_LINE = sub
  {
    # Update collapsed entry with information from current line

    my $clar = shift;         # collapsed-line-array-ref
    my $lar  = shift;         # line-array-ref
    my $v    = $lar->[$fidx]; # value on line that is being collapsed now
    ++$clar->[-1] if $v == 1;
    ++$clar->[-2] if $v == 0;
  };
}

################################################################################

%known = ();
@known = ();

sub add_known
{
  my $key;
  while (defined($key = shift))
  {
    my $val = @known;
    $known{$key} = $val;
    push @known, $key;
    print STDERR "New index $val: $key\n" unless $quiet;
  }
}

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

if (not defined $fidx and not defined $cidx and not defined $midx)
{
  die "at least one of fidx or cidx or midx has to be set";
  exit 1;
}

if (defined $cidx)
{
  $COLLAPSE_NO  = scalar @COLLAPSE_EQ;

  print "Collapse lines - requiring equality in fields: ", join(", ", @COLLAPSE_EQ), "\n";
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

for my $fname (@ARGV)
{
  open L, $fname or die "Can not open $fname";

  reset_file_pos();

  my $l1 = next_line();

  $COLLAPSE_L1_FIX->(\$l1) if defined $COLLAPSE_L1_FIX;

  if (defined $fidx)
  {
    $parser->parse($l1);
    my @es = $parser->fields();
    print "  Field name for fidx=$fidx: $es[$fidx]\n" unless $no_header;
  }

  my @lines = ();
  my $cnt_collapse = 0;

  while (my $l = next_line())
  {
    last if $act eq 'test' and $lineno > 102;

    $parser->parse($l);
    my @es = $parser->fields();

    # Remove front/back whitespace.
    @es = map { s/^\s+//; s/\s+$//; $_; } @es;

    # String to int
    if (defined $fidx)
    {
      $es[$fidx] = uc($es[$fidx]) if $ftoupper;

      if ($ffirstword)
      {
        $es[$fidx] =~ m/([^\s]+)/;
        $es[$fidx] = $1;
      }

      if (not exists $known{$es[$fidx]})
      {
        add_known($es[$fidx]);
      }

      $es[$fidx] = $known{$es[$fidx]};
    }

    # Collapse lines ?

    if (defined $cidx)
    {
      if (scalar @lines > 0 and
          (grep { $lines[-1]->[$_] eq $es[$_] } @COLLAPSE_EQ) == $COLLAPSE_NO)
      {
        $COLLAPSE_NEXT_LINE->($lines[-1], \@es) if defined $COLLAPSE_NEXT_LINE;

        push @{$lines[-1]->[$cidx]}, $es[$cidx];
        ++$cnt_collapse;

        next;
      }
      else
      {
        $COLLAPSE_NEW_LINE->(\@es) if defined $COLLAPSE_NEW_LINE;

        $es[$cidx] = [ $es[$cidx] ];
      }
    }

    # Modify / remap a field?

    if (defined $midx)
    {
      $MODIFY_LINE->(\@es) if defined $MODIFY_LINE;
    }

    push @lines, \@es;
  }
  close L;

  if (defined $cidx)
  {
    printf "Collapsed %d entries, total would be %d ... now %d.\n",
        $cnt_collapse, $lineno, scalar @lines;
  }

  if (defined $fidx)
  {
    my $K = @known;
    for (my $i = 0; $i < $K; ++$i)
    {
      printf "  %2d  %s\n", $i, $known[$i];
    }
  }

  if ($act eq 'do')
  {
    if (defined $sidx)
    {
      @lines = sort { $a->[0] <=> $b->[0] || $a->[$sidx] <=> $b->[$sidx] } @lines;
    }

    open L, ">$fname";
    print L $l1, "\n";
    for $l (@lines)
    {
      if (defined $cidx)
      {
        my @a = @{$l->[$cidx]};
        if ($COLLAPSE_SORT)
        {
          @a = sort @a;
        }
        if ($COLLAPSE_UNIQ)
        {
          my %seen = ();
          @a = grep { ! $seen{$_}++ } @a;
        }
        $l->[$cidx] = $COLLAPSE_SEP . join($COLLAPSE_SEP, @a) . $COLLAPSE_SEP;
      }

      $parser->print(\*L, $l);  print L "\n";
    }
    close L;
  }
}
