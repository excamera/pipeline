all:
			@python -m grpc_tools.protoc -I$(srcdir)/protos --python_out=$(builddir)/service/ --grpc_python_out=$(builddir)/service/ $(srcdir)/protos/pipeline.proto
			@python -m grpc_tools.protoc -I$(srcdir)/protos --python_out=$(builddir)/util/ $(srcdir)/protos/joblog.proto

dist_noinst_DATA = $(srcdir)/protos/pipeline.proto
