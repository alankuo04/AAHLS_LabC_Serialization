#include "top.hpp"
#include <ap_int.h>
#include "hls_stream.h"

#define MAX_FIELD 10

struct Obj{
	int type;
	int valid;
	int fieldID;
	union{
		long long i;
		double d;
		char c[8];
	}data;
};

struct ObjIdx{
	int type;
	int valid;
	int fieldID;
	int index;
	int idx;
	int offset_idx;
	int offset;
	union{
		long long i;
		double d;
		char c[8];
	}data;
};


union Offset{
	char c[4];
	uint32_t i;
};


void ReadFromMem(char* src_buff, int src_sz, hls::stream<Obj>& input_stream)
{
	ap_uint<88> temp;
	Obj obj;
	read: for (int i=0; i<src_sz; i++) {
#pragma HLS PIPELINE II=12
		for (int j=0; j<11; j++) {
			temp.range((11-j)*8-1, (10-j)*8) = src_buff[i*11+j];
		}
		obj.type = temp.range(87, 84);
		obj.valid = temp.range(83, 80);
		obj.fieldID = temp.range(79, 64);
		obj.data.i = temp.range(63, 0);
		input_stream.write(obj);
	}
}

void ChooseColumn(hls::stream<Obj>& input_stream, int src_sz, int field, int offset, hls::stream<ObjIdx>& column_stream)
{
	int index = 0;
	int idx = 0;
	int offset_idx = 0;
	choose: for (int i=0; i<src_sz; i++) {
#pragma HLS PIPELINE II=1
		Obj obj = input_stream.read();
		ObjIdx temp;
		ap_uint<64> t = obj.data.i;

		if (obj.fieldID == field || obj.type == 13 || obj.type == 15)
			temp.type = obj.type;
		else
			temp.type = -1;

		temp.valid = obj.valid;
		temp.fieldID = obj.fieldID;

		if (temp.type == 15) {
			index = 0;
			offset_idx = 0;
		}
		else if (temp.type != -1 && (temp.type == 1 || temp.type ==3))
			index = -1;

		if (temp.type == 1 || temp.type == 3) {
			temp.data.i = obj.data.i;
		}
		else if (temp.type == 5) { // string
			index += temp.valid;
			for (int j=1; j<=8; j++) {
				temp.data.c[j-1] = t.range((j*8)-1, (j-1)*8);
			}
		}
		else if (temp.type == 13 && index != -1) {
			offset_idx += 4;
			temp.data.i = index;
		}
		else if (temp.type == 13) // newline
			temp.data.i = index;

		if (temp.type != -1)
			idx += temp.valid;

		temp.index = index;
		temp.idx = idx;
		temp.offset_idx = offset_idx;
		temp.offset = offset;

		column_stream.write(temp);
	}
}

void WriteToMem(hls::stream<ObjIdx>& column_stream, int src_sz, int max_row, char* dst_buff)
{
	write: for (int i=0; i<src_sz*MAX_FIELD; i++) {
#pragma HLS PIPELINE II=1
		ObjIdx temp = column_stream.read();
		int offset = temp.offset;
		int idx = temp.idx - temp.valid;
		int offset_idx = temp.offset_idx - 4;
		if (temp.type != -1 && offset != -1) {
			if (temp.type == 13  && temp.data.i != -1) {
				Offset off;
				off.i = int(temp.data.i);
				write_offset: for (int j=0; j<4; j++) {
					dst_buff[offset+offset_idx+j+4] = off.c[j];
					}
			}
			else if (temp.type != 13 || temp.type != 15){
				write_data: for (int j=0; j<temp.valid; j++) {
#pragma HLS LOOP_TRIPCOUNT max=8
					if (temp.type == 5) {
						dst_buff[offset+max_row*4+idx+j+4] = temp.data.c[j];
					}
					else {
						dst_buff[offset+idx+j] = temp.data.c[j];
					}
				}
			}
		}
	}
}

void Spilt(int src_sz, hls::stream<Obj>& input, hls::stream<Obj> output[MAX_FIELD])
{
	split: for (int i=0; i<src_sz; i++)
	{
#pragma HLS PIPELINE II=1
		Obj temp = input.read();
		for (int j=0; j<MAX_FIELD; j++) {
			output[j].write(temp);
		}
	}
}

void Merge(int src_sz, hls::stream<ObjIdx>& output, hls::stream<ObjIdx> input[MAX_FIELD])
{
	merge: for (int i=0; i<src_sz; i++)
	{
#pragma HLS PIPELINE II=11
		for (int j=0; j<MAX_FIELD; j++) {
			output.write(input[j].read());
		}
	}
}

void Convert(char* src_buff, int src_sz, char* dst_buff, int max_rows, int offset[MAX_FIELD])
{
#pragma HLS ARRAY_PARTITION variable=offset type=complete
	hls::stream<Obj> input_stream;
	hls::stream<Obj> input_stream_s[MAX_FIELD];
	hls::stream<ObjIdx> column_stream[MAX_FIELD];
	hls::stream<ObjIdx> output_stream;

	#pragma HLS STREAM variable=input_stream depth=100
	#pragma HLS STREAM variable=input_stream_s depth=100
	#pragma HLS STREAM variable=column_stream depth=1000
	#pragma HLS STREAM variable=output_stream depth=100

	#pragma HLS DATAFLOW

	ReadFromMem(src_buff, src_sz, input_stream);
	Spilt(src_sz, input_stream, input_stream_s);
	for (int i=0; i<MAX_FIELD; i++) {
		#pragma HLS UNROLL
		ChooseColumn(input_stream_s[i], src_sz, i, offset[i], column_stream[i]);
	}
	Merge(src_sz, output_stream, column_stream);
	WriteToMem(output_stream, src_sz, max_rows+1, dst_buff);
}

void dut(char* src_buff, int src_sz, char* dst_buff)
{
#pragma HLS INTERFACE mode=m_axi port=src_buff depth=SRC_INTERFACE_SZ
#pragma HLS INTERFACE mode=m_axi port=dst_buff depth=DST_INTERFACE_SZ
#pragma HLS INTERFACE mode=s_axilite port=src_sz

	int max_field = 0;
	int max_rows = 0;
	int size = src_sz/11;
	ap_uint<88> temp;
	Obj obj;
	int offset[MAX_FIELD] = {0};
	int check[MAX_FIELD] = {0};

	for (int i=0; i<size; i++) {
#pragma HLS PIPELINE II=12
		for (int j=0; j<11; j++) {
			temp.range((11-j)*8-1, (10-j)*8) = src_buff[i*11+j];
			dst_buff[i*11+j] = 0;
		}
		obj.type = temp.range(87, 84);
		obj.valid = temp.range(83, 80);
		obj.fieldID = temp.range(79, 64);
		if (obj.fieldID > max_field)
			max_field = obj.fieldID;
		if (obj.type == 13)
			max_rows += 1;
		else if (obj.type == 5)
			check[obj.fieldID] = 1;
		if (obj.type != 13 && obj.type != 15)
			offset[obj.fieldID+1] += obj.valid;
	}
	for (int i=0; i<MAX_FIELD; i++) {
		if (i > max_field)
			offset[i] = -1;
		else
			if (check[i])
				offset[i+1] += offset[i]+(max_rows+2)*4;
			else
				offset[i+1] += offset[i];

	}
	Convert(src_buff, size, dst_buff, max_rows, offset);
}
