import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;
public class DBOutputWritable implements DBWritable{
    private String starting_phrases;
    private String predicted_word;
    private int count;

    DBOutputWritable(String starting_phrases, String predicted_word, int count) {
        this.starting_phrases = starting_phrases;
        this.predicted_word = predicted_word;
        this.count = count;
    }

    //@Override
    public void readFields(ResultSet rs) throws SQLException {
        starting_phrases = rs.getString(1);
        predicted_word = rs.getString(2);
        count = rs.getInt(3);
    }

   // @Override
    public void write(PreparedStatement ps) throws SQLException {
        ps.setString(1, starting_phrases);
        ps.setString(2, predicted_word);
        ps.setInt(3, count);
    }
}
