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
public class Var extends Text {
	public Var() {
		super();
	}
	public Var(String v) {
		super(v);
	}
	public Var(int v) {
		super(String.valueOf(v));
	}
	public Var(long v) {
		super(String.valueOf(v));
	}
	public Var(float v) {
		super(String.valueOf(v));
	}

	public Var(Integer v) {
		super(String.valueOf(v.intValue()));
	}
	public Var(Long v) {
		super(String.valueOf(v.longValue()));
	}
	public Var(Double v) {
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
