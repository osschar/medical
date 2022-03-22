sub ArgParse
{
    my $quiet = shift;

    while (1)
    {
        if ($ARGV[0] =~ m/\w+\.cfg/)
        {
            die "config file $ARGV[0] not readable" unless -r $ARGV[0];
            print STDERR "importing config from $ARGV[0]\n";
            my $ret = do "./$ARGV[0]";
            die "failed parsing $ARGV[0]: $@" if $@;
            die "couldn't do $ARGV[0]: $!" unless defined $ret;
            die "couldn't run $ARGV[0]"    unless $ret;
            shift @ARGV;
        }
        elsif ($ARGV[0] =~ m/^(\w+)=(-?\d+)$/)
        {
            print STDERR "eval \$$1=$2\n" unless $quiet;
            eval "\$$1=$2";
            shift @ARGV;
        }
        elsif ($ARGV[0] =~ m/^\{.*\}$/)
        {
            print STDERR "eval $ARGV[0]\n" unless $quiet;
            eval $ARGV[0];
            die "eval failed $@" if $@;
            shift @ARGV;
        }
        else
        {
            last;
        }
    }
}

1;
