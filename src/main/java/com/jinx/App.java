package com.jinx;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Author jinx
 */
public class App 
{

    private DateFormat dateFormat = new SimpleDateFormat("HHmmss");
    private int[]  usr_ids = new int[1000];
    private int[] movie_ids = new int[10000];
    private int[] occ=new int[21];
    private BufferedWriter out;
    public static void main( String[] args )
    {
        String path="D:\\Gdisk\\spark\\spark_v1\\log\\";
        App app = new App();
        app.writeFileSpark(path);
        app.writeFileSpark(path,1);
        app.writeFileSpark(path,1,2);
        app.writeFileSpark(path,1,2,3);

    }

    /*
    生成评分数据文件：usrId + movieId +ration + time
     */
    private void writeFileSpark(String path){

        int[] levels=new int[5];
        String[] times=new String[1000];
        for (int i = 0; i <10000; i++) {
            if (i<1000){
                usr_ids[i]=90000000+(int)(Math.random()*100000);
                Timestamp timestamp = new Timestamp((long) (Math.random()*100000000));
                times[i]=dateFormat.format(timestamp);
            }
            if(i<5){
                levels[i]=(int)(Math.random()*5);
            }
            movie_ids[i]=(int)(Math.random()*10000);
        }

        try {
             out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path+"ratings.log"),StandardCharsets.UTF_8));
            for (int i = 0; i < usr_ids.length; i++) {
                String temp=usr_ids[i]+"::"+movie_ids[i]+"::"+levels[(int) (Math.random()*5)]+"::"+times[i];
                out.write(temp+"\n");
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
    生成用户信息文件：usrId + FM + age + occ + post
     */
    private void writeFileSpark(String path,int t){

        String[] males=new String[]{"F","M"};
        int[] ages=new int[]{1,18,25,30,35,40,45,50,56};

        int[] youbian=new int[1000];
        for (int i = 0; i <1000; i++) {
            youbian[i]= (int) (Math.random()*10000);
            if(i<21){
                occ[i]=i;
            }
        }

        try {
             out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path+"user.log"),StandardCharsets.UTF_8));
            for (int i = 0; i < usr_ids.length; i++) {
                String temp=usr_ids[i]+"::"+males[(int) (Math.random()*2)]+"::"+ages[(int) (Math.random()*9)]+"::"+occ[(int) (Math.random()*21)]+"::"+youbian[i];
                out.write(temp+"\n");
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
    /*
    生成电影信息文件：movieId + name + type
     */
    private void writeFileSpark(String path,int t,int r){
        String pip="wrgofsdjfinvoiFOKDNDSJFIOWEJF";
        String[] movie_type=new String[]{"Action","Adventure","Animation","Childdren","Comedy","Crime","Documentary",
                "Drama","Fantasy","Film-Noir","Horror","Musical","Mystety","Romance","Sci-Fi","Thriller","War","Western"};
        HashSet<String> movie_names = new HashSet<>();
        HashSet<Integer> movieSet = new HashSet<>();
        for (int movieId : movie_ids) {
            movieSet.add(movieId);
        }


        while (movie_names.size()!=movieSet.size()){
            StringBuilder temp= new StringBuilder();
            for (int i = 0; i <5; i++) {
                temp.append(pip.charAt((int) (Math.random() * pip.length())));
            }
            movie_names.add(temp.toString());
        }

        try {
             out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path+"movies.log"),StandardCharsets.UTF_8));
            Iterator<String> iterator = movie_names.iterator();
            Iterator<Integer> Set = movieSet.iterator();
            for (int i = 0; i < movieSet.size(); i++) {
                String movie="";
                int movieid = 0;
                if(iterator.hasNext()){
                    movie=iterator.next();
                }
                if(Set.hasNext()){
                    movieid=Set.next();
                }

                String temp=movieid+"::"+movie+"::"+movie_type[(int) (Math.random()*movie_type.length)];
                out.write(temp+"\n");
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
    生成地址信息：id + name
     */
    private void writeFileSpark(String path,int t,int y,int q){

        String[] occ_names=new String[]{"其他","教育家","艺术家","文书","高校毕业生","客户服务","医生","行政",
                "农民","家庭主妇","中小学生","律师","程序员","退休","销售","科学家","个体户","技术员","商人","失业","作家"};
        try {
             out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path+"occupation.log"), StandardCharsets.UTF_8));
            for (int i = 0; i < occ.length; i++) {
                String temp=occ[i]+"::"+occ_names[i];
                out.write(temp+"\n");
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
