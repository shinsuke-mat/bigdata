package posmining.enshu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import posmining.utils.Var;
import posmining.utils.PosUtils;

/**
 * 全おにぎりの販売個数を出力する
 * @author shin
 *
 */
public class OnigiriCount {

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		PosUtils.setupRuntimeEnvironment();
		
		// MapperクラスとReducerクラスを指定
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(OnigiriCount.class);            // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("shinsuke");                        // ★自分の名前

		// 入出力フォーマットをテキストに指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// MapperとReducerの出力の型を指定
		job.setMapOutputKeyClass(Var.class);
		job.setMapOutputValueClass(Var.class);
		job.setOutputKeyClass(Var.class);
		job.setOutputValueClass(Var.class);

		// 入出力ファイルを指定
		String inputpath = "posdata/*.csv";
		String outputpath = "out/onigiriCount";           // ★MRの出力先
		if (args.length > 0) {
			inputpath = args[0];
		}

		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		// 出力フォルダは実行の度に毎回削除する（上書きエラーが出るため）
		PosUtils.deleteOutputDir(outputpath);

		// Reducerで使う計算機数を指定
		job.setNumReduceTasks(4);

		// MapReduceジョブを投げ，終わるまで待つ．
		job.waitForCompletion(true);
	}


	// Mapperクラスのmap関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, Var, Var> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split(",");

			// おにぎりでないレシートは無視
			if (csv[PosUtils.ITEM_CATEGORY_NAME].equals("おにぎり・おむすび") == false) {
				return;
			}

			// valueとなる販売個数を取得
			String count = csv[PosUtils.ITEM_COUNT];

			// emitする （emitデータはVarオブジェクトに変換すること）
			context.write(new Var("onigiri"), new Var(count));
		}
	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<Var, Var, Var, Var> {
		protected void reduce(Var key, Iterable<Var> values, Context context) throws IOException, InterruptedException {

			// 売り上げを合計
			int count = 0;
			for (Var value : values) {
				count += value.toInt();
			}

			// emit
			context.write(key, new Var(count));
		}
	}
}
