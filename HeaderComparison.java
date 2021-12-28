import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.text.similarity.JaroWinklerDistance;

public class HeaderComparison {
	public static void main(String[] args) {
		String inputFolderPath = "C:\\HeaderProblem";
		HeaderComparison objHeaderComparison = new HeaderComparison();
		objHeaderComparison.headerProblemSolution(inputFolderPath);
	}

	public String readFromFile(String filePath) {
		try {
			File file = new File(filePath);
			BufferedReader br = new BufferedReader(new FileReader(file));
			String st;
			while ((st = br.readLine()) != null)
			return st;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

		}
		return null;
	}

	public void headerProblemSolution(String inputFolderPath) {
		String newheaderStr =inputFolderPath+"\\newheader.csv";
		String baseHeaderstr = inputFolderPath+"\\baseHeader.csv";
		
		String newheader= readFromFile(newheaderStr);
		String baseHeader= readFromFile(baseHeaderstr);

		
		String newheaderArr[] = newheader.split(",");
		String baseHeaderArr[] = baseHeader.split(",");
		double simillarityPercenateThreshold = 0.8;
		ArrayList<HeaderVO> newBaseHeaderArray = new ArrayList<HeaderVO>();

		JaroWinklerDistance distance = new JaroWinklerDistance();
		for (String baseHeaderWord : baseHeaderArr) {
			ArrayList<HeaderVO> compareArray = new ArrayList<HeaderVO>();
			for (String newheaderWord : newheaderArr) {
				double matchPer = distance.apply(baseHeaderWord, newheaderWord);
				compareArray.add(new HeaderVO(baseHeaderWord, newheaderWord, matchPer));
			}
			if (compareArray != null && !compareArray.isEmpty()) {
				List<HeaderVO> thresholdArray = compareArray.stream()
						.filter(val -> val.getMatchPercentage() > simillarityPercenateThreshold)
						.collect(Collectors.toList());
				if (thresholdArray != null && !thresholdArray.isEmpty()) {
					Comparator<HeaderVO> comparator = Comparator.comparing(HeaderVO::getMatchPercentage);
					HeaderVO headerVO = thresholdArray.stream().max(comparator).get();
//					System.out.println(headerVO.getNewheader() + " , " + headerVO.getBaseHeader() + " , " + headerVO.getMatchPercentage());
					newBaseHeaderArray.add(headerVO);
				} else {
//					System.out.println("base_Header : "+baseHeaderWord + " , No Match");
					newBaseHeaderArray.add(new HeaderVO(baseHeaderWord, "' '", 0));
				}
			}
		}
		if (newBaseHeaderArray != null && !newBaseHeaderArray.isEmpty()) {
			for (String newheaderWord : newheaderArr) {
				boolean valueExist = newBaseHeaderArray.stream()
						.anyMatch(val -> val.getNewheader().equalsIgnoreCase(newheaderWord));
				if (!valueExist) {
					newBaseHeaderArray.add(new HeaderVO("", newheaderWord, 0));
				}
			}
		}
		StringBuilder strBuilder = new StringBuilder();
		
		for (HeaderVO headerVo : newBaseHeaderArray) {
			System.out.println(
					headerVo.getNewheader() + " , " + headerVo.getBaseHeader() + " , " + headerVo.getMatchPercentage());
			strBuilder.append(headerVo.getNewheader()).append(",");
		}

		String strCSVFinal= strBuilder.toString().substring(0, strBuilder.length()- 1);  

		System.out.println("final new header: "+strCSVFinal);
		writeFile(inputFolderPath, strCSVFinal);
		
	}

	public void writeFile(String inputFolderPath,String content){
        try {
            FileWriter fWriter = new FileWriter(inputFolderPath+"\\newBaseHeader.csv");
            fWriter.write(content);
            fWriter.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
	}
}
