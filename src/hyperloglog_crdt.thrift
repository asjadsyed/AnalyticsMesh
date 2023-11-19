service HyperLogLogCRDT {
	oneway void push(1: binary other_serialized)
	binary pull()
}
