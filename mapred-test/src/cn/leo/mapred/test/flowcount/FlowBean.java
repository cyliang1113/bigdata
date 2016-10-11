package cn.leo.mapred.test.flowcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable {
	private String cellphoneNo;
	private Long upFlow;
	private Long downFlow;
	private Long sumFlow;

	public void set(Long upFlow, Long downFlow, String cellphoneNo) {
		this.cellphoneNo = cellphoneNo;
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}

	public String getCellphoneNo() {
		return cellphoneNo;
	}

	public void setCellphoneNo(String cellphoneNo) {
		this.cellphoneNo = cellphoneNo;
	}

	public Long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(Long upFlow) {
		this.upFlow = upFlow;
	}

	public Long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(Long downFlow) {
		this.downFlow = downFlow;
	}

	public Long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(Long sumFlow) {
		this.sumFlow = sumFlow;
	}

	@Override
	public String toString() {
		return this.cellphoneNo + "\t" + this.upFlow + "\t" + this.downFlow + "\t" + this.sumFlow;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		cellphoneNo = in.readUTF();
		upFlow = in.readLong();
		downFlow = in.readLong();
		sumFlow = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(cellphoneNo);
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

}
