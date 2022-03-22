#!/usr/bin/perl

for $i (@ARGV)
{
    push @decl, sprintf "DECL(%s, %d, %s)", $i, $cnt, lc($i);
    push @init, sprintf "m_rt_files[%d] = new RtFile<$i>(\"$i\")", $cnt;
    ++$cnt;
}

print "// Declaration of table variables\n";
print "#define RT_SET_SHORTHANDS_DECL \\\n  ", join("; \\\n  ", @decl), "\n";
print "\n";

print "// Initialization of table vector\n";
print "#define RT_SET_INIT_TABLE_VECTOR \\\n",
      "  m_rt_files.resize(", scalar @ARGV, "); \\\n  ",
      join("; \\\n  ", @init), "\n";
