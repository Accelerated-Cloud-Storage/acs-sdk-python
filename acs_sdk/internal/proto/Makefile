.PHONY: proto pythonproto

proto:
	# Generate Python code
	python3 -m grpc_tools.protoc \
			--python_out=../generated \
			--grpc_python_out=../generated \
			--proto_path=. \
			*.proto