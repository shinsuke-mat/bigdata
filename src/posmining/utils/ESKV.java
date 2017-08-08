package posmining.utils;

import org.apache.hadoop.io.Text;

/**
 * HadoopのKeyとValueで用いる汎用的なオブジェクト
 *
 * org.apache.hadoop.io.Textのラッパークラス
 *
 * @author shin
 *
 */
public class ESKV extends Text {
	public ESKV() {
		super();
	}
	public ESKV(String v) {
		super(v);
	}
	public ESKV(int v) {
		super(String.valueOf(v));
	}
	public ESKV(long v) {
		super(String.valueOf(v));
	}
	public ESKV(float v) {
		super(String.valueOf(v));
	}

	public ESKV(Integer v) {
		super(String.valueOf(v.intValue()));
	}
	public ESKV(Long v) {
		super(String.valueOf(v.longValue()));
	}
	public ESKV(Double v) {
		super(String.valueOf(v.doubleValue()));
	}

	public String toString() {
		return super.toString();
	}
	public Integer toInt() {
		return Integer.parseInt(this.toString());
	}
	public Long toLong() {
		return Long.parseLong(this.toString());
	}
	public Double toDouble() {
		return Double.parseDouble(this.toString());
	}
}
