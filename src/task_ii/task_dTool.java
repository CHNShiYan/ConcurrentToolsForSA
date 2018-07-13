package task_ii;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class task_dTool implements Writable,DBWritable{
	
	String key;
	String type;
	Double average;

	public task_dTool(String key, String type, Double average) {
		// TODO Auto-generated constructor stub
		this.key=key;
		this.type=type;
		this.average=average;
	}

	@Override
	public void readFields(ResultSet rs) throws SQLException {
		// TODO Auto-generated method stub
		this.key=rs.getString(1);
		this.type=rs.getString(2);
		this.average=rs.getDouble(3);
	}

	@Override
	public void write(PreparedStatement ps) throws SQLException {
		// TODO Auto-generated method stub
		ps.setString(1, this.key);
		ps.setString(2, this.type);
		ps.setDouble(3, this.average);
	}

	@Override
	public void readFields(DataInput i) throws IOException {
		// TODO Auto-generated method stub
		this.key=i.readUTF();
		this.type=i.readUTF();
		this.average=i.readDouble();
	}

	@Override
	public void write(DataOutput o) throws IOException {
		// TODO Auto-generated method stub
		o.writeUTF(key);
		o.writeUTF(type);
		o.writeDouble(average);
	}
	

}
