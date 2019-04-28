package com.beicai.mapreduce.recommend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Movie implements WritableComparable<Movie>{
	
		private String movie;
		private float score;
		public String getMovie() {
			return movie;
		}
		public void setMovie(String movie) {
			this.movie = movie;
		}
		public float getScore() {
			return score;
		}
		public void setScore(float score) {
			this.score = score;
		}
		public Movie(String movie, float score) {
			super();
			this.movie = movie;
			this.score = score;
		}
		@Override
		public int hashCode() {
			// TODO Auto-generated method stub
			return super.hashCode();
		}
		@Override
		public boolean equals(Object obj) {
			// TODO Auto-generated method stub
			return super.equals(obj);
		}
		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return movie + "\t" + score;
		}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(movie);
			out.writeFloat(score);
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			movie = in.readUTF();
			score = in.readFloat();
		}
		@Override
		public int compareTo(Movie o) {
			return -(int)(this.score*1000 - o.score*1000);
		}
		
	
}
