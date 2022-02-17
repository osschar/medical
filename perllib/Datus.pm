package Datus;

my $months = [ [ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 ],
            [ 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 ] ];

my $cum_months = [ [ 0 ], [ 0 ] ];

for ($i = 0; $i < 12; ++$i)
{
  for ($j = 0; $j < 2; ++$j)
  {
    push @{$cum_months->[$j]}, $cum_months->[$j][$i] + $months->[$j][$i];
  }
}

# print join(" ", @{$cum_months->[0]}), "\n";
# print join(" ", @{$cum_months->[1]}), "\n";

sub date_to_double
{
  my  $date = shift;
  my  $extra_day = shift;

  my ($m, $d, $y) = $date =~ m!(\d+)/(\d+)/(\d+)!;

  if ($y < 100)
  {
    if ($y < 20) { $y += 2000; } else { $y += 1900; }
  }

  my $leapp = ($y % 4 == 0 and $y % 100 != 0 or $y % 400 == 0);

  $d += $cum_months->[$leapp][$m - 1];

  return $y + ($d - 1 + $extra_day) / $cum_months->[$leapp][12];
}

sub datetime_to_double
{
  my $txt = shift;  # e.g.: 09/21/2016  8:20 AM

  my ($date, $hour, $min, $ampm) = $txt =~ m!(\d+/\d+/\d+)\s+(\d+):(\d+)(?:\s+(A|P)M)?!;

  $hour += 12 if $ampm eq 'P';

  return date_to_double($date, ($hour + $min / 60) / 24);
}

sub time_to_double
{
  my $txt = shift;  # 8:20:27

  my ($hour, $min, $sec) = $txt =~ m!(\d+):(\d+):(\d+)!;

  return $hour + (60*$min + $sec) / 3600;
}

1;
