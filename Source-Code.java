package project;
//importing all necessary files;
import static java.util.Arrays.asList;
import java.util.List;
import java.util.Scanner;
import java.util.Arrays;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

//main class
public class mongodbproject {
	public static void main(String[] args) {
		MongoClient mongoclient = new MongoClient("localhost", 27017);//connecting to mongodb

		DB db = mongoclient.getDB("test");//connected to database test

		DBCollection mycollection  = db.getCollection("employees");//connected to employees collection
		
		while(true)
		{
		Scanner sc = new Scanner(System.in);
		System.out.println();
		System.out.println();
		System.out.println("Enter 1 to find count And Percentage of trainees Failed In First Term");
		System.out.println("Enter 2 to find trainess failed In Aggregate");
		System.out.println("Enter 3 to find average Score of Trainees For Term One");
		System.out.println("Enter 4 to find average Score of Trainees For Aggregate ");
		System.out.println("Enter 5 to find number Of People Failed In All Three Term");
		System.out.println("Enter 6 to find number Of People Failed In Atleast one Term");
		System.out.println("Enter 0 to exit");
		System.out.println("Enter choice: ");
		int choice = sc.nextInt(); // user input for requirements
		//switch loop to perform each requirement according to user choice
		switch(choice)
		{
		case 1:
		{
		countAndPercentageFailedInFirstTerm(mycollection);
		break;
		}
		case 2:
		{
		failedInAggregate(mycollection);
		break;		
		}
		case 3:
		{
		averageScoreofTraineesForTermOne(mycollection);
		break;
		}
		case 4:
		{
		averageScoreofTraineesForAggregate(mycollection);
		break;
		}
		case 5:
		{
		numberOfPeopleFailedInAllThreeTerm(mycollection);
		break;
		}
		case 6:
		{
		numberOfPeopleFailedInAnyoneTerm(mycollection);
		break;
		}
		case 0:
		{
			System.out.println("exiting...");
			System.exit(0);
		}
		}
		}
	}
	//Trainees who failed in aggregate (given below)
	private static void failedInAggregate(DBCollection mycollection) 
	{
		BasicDBObject unwind = new BasicDBObject("$unwind","$results");
		BasicDBObject groupfields = new BasicDBObject("_id","$name");
		groupfields.append("sum", new BasicDBObject("$sum","$results.score"));
		DBObject group = new BasicDBObject("$group",groupfields);
		BasicDBObject match = new BasicDBObject("$match",new BasicDBObject("sum", new BasicDBObject("$lt",111)));
		List<DBObject> pipeline = Arrays.asList(unwind,group,match);
		AggregationOutput output = mycollection.aggregate(pipeline);
		for(DBObject result : output.results())
		{
			System.out.println("Trainees who failed in aggregate (term1+ term2 + term3) : "+ result);
		}
		
		
	}
	//Average Score of Trainees for aggregate i.e. for each term
	private static void averageScoreofTraineesForAggregate(DBCollection mycollection) 
	{
		BasicDBObject unwind = new BasicDBObject("$unwind","$results");//unwinding each document
		BasicDBObject groupfields = new BasicDBObject("_id","$name");
		groupfields.append("avg", new BasicDBObject("$avg","$results.score"));
		DBObject group = new BasicDBObject("$group", groupfields);//grouping
		List <DBObject> pipeline = Arrays.asList(unwind,group);
		AggregationOutput output = mycollection.aggregate(pipeline);//aggregation used
		
		for(DBObject result : output.results())
		{
			System.out.println("Average score of trainees for aggregate (term1+ term2 + term3) : "+ result);
		}
		
	}
	//Average score of Trainees for Term one only
	private static void averageScoreofTraineesForTermOne(DBCollection mycollection) 
	{
		Long count = mycollection.count();
		BasicDBObject select = new BasicDBObject("$match",new BasicDBObject("results.evaluation","term1"));//matching the specific requirement
		BasicDBObject unwind = new BasicDBObject("$unwind", "$results");//unwinding
		BasicDBObject groupfields = new BasicDBObject("_id","$name");
		groupfields.append("avg", new BasicDBObject("$avg","$results.score"));
		DBObject group = new BasicDBObject("$group", groupfields);//grouping
		List <DBObject> pipeline = Arrays.asList(unwind,select,group);
		AggregationOutput output = mycollection.aggregate(pipeline);//aggregation used
		Double num=0.0;
		for(DBObject result : output.results())
		{
			num =num + Double.valueOf(result.get("avg").toString());
		}
		System.out.println("Average score of trainees for term1 : "+ (num/count));
	}

//Number of people failed in all three terms i.e. scored less than 37 in each term
	private static void numberOfPeopleFailedInAllThreeTerm(DBCollection mycollection) 
	{
		Long FinALL = mycollection.count(new BasicDBObject("results.0.score", new BasicDBObject("$lt",37)).append("results.1.score", new BasicDBObject("$lt",37)).append("results.2.score", new BasicDBObject("$lt",37)));
		System.out.println("Total number of trainees failed in all three terms are : " + FinALL);
	}

//count and percentage of people failed in only first term.
	private static void countAndPercentageFailedInFirstTerm(DBCollection mycollection)
	{
		Long totalemp = mycollection.count();
		System.out.println("Total number of employees in collection are: " + totalemp);
		Long count = mycollection.count(new BasicDBObject("results.0.score", new BasicDBObject("$lt",37)));
		System.out.println("Total number of employees failed in first term are : " + count);
		double per_fail = ((count*100)/totalemp);
		System.out.println("Percentage of employees failed in first term are: " + per_fail);	
	}

//number of people failed in atleast one term
	private static void numberOfPeopleFailedInAnyoneTerm(DBCollection mycollection)
	{
		Long FATONCE = mycollection.count(new BasicDBObject("results.score", new BasicDBObject("$lt",37)));
		System.out.println("Total number of employees failed in any of the three terms are: " + FATONCE);
	}

}
