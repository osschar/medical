package Date;

# Initialization of internal tables

my($YEAR, $MONTH, $DAY) = (0 .. 2);

@daycount = (
    [ 365, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 ],
    [ 366, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 ]
);

@addup = ( );
{
    my $s0 = 0; my @a0 = (0); my $s1 = 0; my @a1 = (0);
    for($i=1; $i<=12; $i++) {
	push(@a0, $s0); $s0 += $daycount[0][$i];
	push(@a1, $s1); $s1 += $daycount[1][$i];
    }
    $addup[0] = [ @a0 ];
    $addup[1] = [ @a1 ];
}

@adddown = ( );
{
    my $s0 = 365; my @a0 = (365); my $s1 = 366; my @a1 = (366);
    for($i=1; $i<=12; $i++) {
	$s0 -= $daycount[0][$i]; push(@a0, $s0);
	$s1 -= $daycount[1][$i]; push(@a1, $s1);
    }
    $adddown[0] = [ @a0 ];
    $adddown[1] = [ @a1 ];
}

%mname2int = (
  'Jan' =>  1, 'Feb' =>  2, 'Mar' =>  3, 'Apr' =>  4, 'May' =>  5, 'Jun' =>  6,
  'Jul' =>  7, 'Aug' =>  8, 'Sep' =>  9, 'Oct' => 10, 'Nov' => 11, 'Dec' => 12
);

$date_mname_re = "(\\d+)-(" . join("|", keys %mname2int) . ")-(\\d+)";

#for $i ( 0 .. $#addup ) {
#    print "\t [ @{$adddown[$i]} ],\n";
#}

# "Internal functions" ... or what the fuck ... go and use them

sub isleap {
    my $y = shift;
    return 1 unless $y%400;
    return 0 unless $y%100;
    return 1 unless $y%4;
    return 0;
}

sub isvalid {
    my $self = shift;
    return 0 if( $self->[$YEAR] <= 0 );
    return 0 if( $self->[$MONTH] < 1 || $self->[$MONTH] > 12);
    return 0 if( $self->[$DAY] < 1 ||
		 $self->[$DAY] > $daycount[isleap($self->[$YEAR])][$self->[$MONTH]]);
    return 1;
}

sub cmp
{
    my $d1 = shift;
    my $d2 = shift;

#   This is supposed to be fast
    if($d1->[$YEAR] > $d2->[$YEAR]) { return 1; }
    if($d1->[$YEAR] < $d2->[$YEAR]) { return -1; }
    if($d1->[$MONTH] > $d2->[$MONTH]) { return 1; }
    if($d1->[$MONTH] < $d2->[$MONTH]) { return -1; }
    if($d1->[$DAY] > $d2->[$DAY]) { return 1; }
    if($d1->[$DAY] < $d2->[$DAY]) { return -1; }
    return 0;
}

sub daysinyear
{
    my $y = shift;
    return $daycount[isleap($y)][0];
}


# Returns nof day in year: [$dayinyear=$d->dayinyear;]
sub dayinyear
{
    my $self = shift;
    return $addup[isleap($self->[$YEAR])][$self->[$MONTH]] + $self->[$DAY];
}

# Returns nof days till 31.12.year: [$days=$d->daystillend;]
sub daystillend
{
    my $self = shift;
    my $il = isleap($self->[$YEAR]);
    return $adddown[$il][$self->[$MONTH]-1] - $self->[$DAY];
}

# Returns nof days from one date to the other:
# [$days=$d1->days($d2); ... or however]
# Please note: days = $d1 - $d2; and so $d1<$d2 => days < 0
sub days
{
    my $d1 = shift;
    my $d2 = shift;
    my ($y1, $y2, $yy, $cval, $sum);

    $cval = &cmp($d1, $d2);
    if( $cval == -1 ) {
	$y1 = $d1; $y2 = $d2;
    } elsif( $cval == 1 ) {
	$y1 = $d2; $y2 = $d1;
    } else {
	return 0;
    }
    $yy = $y1->[$YEAR]; $sum = 0;
    while( $yy < $y2->[$YEAR] ){
	$yy++;
	$sum += &daysinyear($yy);
    }
    return $cval*($sum + $y1->daystillend - $y2->daystillend);
}

# Add/subtract nof days and return a reference to a new Date:
# [$e=$d->adddays(20);]
sub adddays
{
    my $self = shift;
    my $days = shift;

    my ($isl, $dout, $dc, $i);

    if($days > 0) {
	$days += $self->dayinyear;
	$dout = Date->new( $self->[$YEAR], 1, 1);
	while( $days > ($dc = $daycount[isleap($dout->[$YEAR])][0]) ) {
	    $dout->[$YEAR]++;
	    $days -= $dc;
	}
	$i = 12; $isl = isleap($dout->[$YEAR]); 
	while( ($dc = $addup[$isl][$i]) >= $days ) {
	    $i--;
	}
	$dout->[$MONTH] = $i; $dout->[$DAY] = $days - $dc;
    } else {
	$days = -$days;
	$days += $self->daystillend;
	$dout = Date->new( $self->[$YEAR], 12, 31);
	while( $days > ($dc = $daycount[isleap($dout->[$YEAR])][0]) ) {
	    $dout->[$YEAR]--;
	    $days -= $dc;
	}
	$i = 1; $isl = isleap($dout->[$YEAR]); 
	while( ($dc = $adddown[$isl][$i]) > $days ) {
	    $i++;
	}
	$dout->[$MONTH] = $i; $dout->[$DAY] = $daycount[$isl][$i] - ($days - $dc);
    }
    return $dout;
}

# Methods

sub new
{
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = [];

    if ($#_ == 0) {		# One argument
	if( ref($_[0]) eq 'Date' ) { # A Date was passed
	    @_ = @{$_[0]};
	} else {		     # Hopefully a string<8>
	    @_ = $_[0] =~ m/(\d{4})(\d{2})(\d{2})/;
	}
    } elsif($#_ == 2) {		# Three arguments
	# Empty ... the arguments are in place
    } else {
	@_ = split(/ /, `date +"%Y %m %d"`);
    }
    $self->[$YEAR] = $_[0];
    $self->[$MONTH] = $_[1];
    $self->[$DAY] = $_[2];
	
    bless($self, $class);
    if( $self->isvalid ) {
	return $self;
    } else {			# Destruct
	return 0;
    }
}

sub prdashcompact
{
    my $self = shift;
    return sprintf("%04d-%02d-%02d", $self->[$YEAR], $self->[$MONTH], $self->[$DAY]);
}

sub prcompact
{
    my $self = shift;
    return sprintf("%04d%02d%02d", $self->[$YEAR], $self->[$MONTH], $self->[$DAY]);
}

sub sccompact
{
    my $self = shift;
    @$self = $_[0] =~ m/(\d{4})(\d{2})(\d{2})/;
    if( $self->isvalid ) {
	return $self;
    } else {			# Destruct
	return 0;
    }
}

sub prhuman
{
    my $self = shift;
    return sprintf("%d.%d.%d", $self->[$DAY], $self->[$MONTH], $self->[$YEAR]);
}

sub schuman
{
    my $self = shift;
    $_[0] =~ m/(\d{1,2})[.:](\d{1,2})[.:](\d{1,4})/;
    @$self = ($3, $2, $1);
    if( $self->isvalid ) {
	return 1;
    } else {			# Destruct
	return 0;
    }
}

################################################################

# Change this from outside if needed.
$ref_date = new Date (2000, 1, 1);

my $mindays =  99999;
my $maxdays = -99999;

my $mindate = "undef";
my $maxdate = "undef";

sub days_from_reference
{
  my $txt = shift;
  my $undef_return = shift;

  my ($y, $m, $d);

  if ($txt =~ m!^(\d+)/(\d+)/(\d+)$!)
  {
    $y = $3; $m = $1; $d = $2;
    $y += 2000 if $y < 1000;
  }
  elsif ($txt =~ m!^(\d+)-(\d+)-(\d+)$!)
  {
    $y = $1; $m = $2; $d = $3;
  }
  elsif ($txt =~ m!^${date_mname_re}$!)
  {
    $y = $3; $m = $mname2int{$2}; $d = $1;
    $y += 2000 if $y < 1000;
  }
  else
  {
    # die "date $txt does not match a known format";
    return $undef_return;
  }

  my $date = new Date($y, $m, $d);
  my $dday = $date->days($ref_date);

  if ($dday < $mindays) { $mindays = $dday; $mindate = $txt; }
  if ($dday > $maxdays) { $maxdays = $dday; $maxdate = $txt; }

  return $dday;
}

sub report_min_max_days
{
  return "Date::report_min_max_days: min $mindays for '$mindate', max $maxdays for '$maxdate'.";
}

1;
