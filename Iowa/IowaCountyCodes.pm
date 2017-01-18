package IowaCountyCodes;

my %abbrev2full = (
		   'BK' => 'Black Hawk',
		   'PE' => 'Pottawattamie',
		   'PK' => 'Polk',
		   'ST' => 'Scott',
		   'SY' => 'Story',
		   'WY' => 'Woodbury',
		  );


sub abbrev2full {
  my $code = shift;
  return $abbrev2full{$code};
}

1;
