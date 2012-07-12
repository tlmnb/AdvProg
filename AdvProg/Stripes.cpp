#include <algorithm>
#include <limits>
#include <string>
#include <sstream>
#include <map>


#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"


class StripesMapper : public HadoopPipes::Mapper {
    public:
        StripesMapper( HadoopPipes::TaskContext& context ) {
        }
        
        void map(HadoopPipes::MapContext& context) {
            
            std::string line = context.getInputValue();
            std::vector<std::string> numbers = HadoopUtils::splitString(line, " ");
            std::map<std::string, int> outer;
            std::map<std::string, int> inner;
            
            for(int i=0; i<numbers.size(); i++) {
				std::string s = numbers[i];
				outer.clear();
				for(int j=0; j<numbers.size(); j++) {
					std::string t = numbers[j];
					double count = 1.0;
					
				}
            }
            
        }
        
        
        std::string mapToStr(std::map<std::string,int> x) {
            std::stringstream ss;
            std::map<std::string,int>::iterator it;
            for(it = x.begin(); it != x.end(); it++) {
                ss << "(" << it->first << "," << it->second << ");";
            }
            return ss.str();
        }

};

class StripesReducer : public HadoopPipes::Reducer {
    public:
        double count;
        StripesReducer( HadoopPipes::TaskContext& context ) {
            this->count = 0.0;
        }
        
        void reduce(HadoopPipes::ReduceContext& context) {
            std::string key = context.getInputKey();
            
            std::map<std::string,int> sumMap;
            while(context.nextValue()) {
                count++;
                std::string value = context.getInputValue(); //textual representation of the map
                std::vector<std::string> pairs = HadoopUtils::splitString(value, ";");
                std::vector<std::string>::iterator it;
                for(it = pairs.begin(); it != pairs.end(); it++) {
                        std::string x = *it;
                        std::vector<std::string> pair = HadoopUtils::splitString(x,",");
                        std::string k = pair[0].erase(0,1); // delete trailing '('
                        std::string v = pair[1].erase(pair[1].size()-1); //delete ')'
                        int vInt = HadoopUtils::toInt(v);
                        if(sumMap.find(k)==sumMap.end()) {
                            sumMap[k]=vInt;
                        } else {
                            sumMap[k]=sumMap[k]+vInt;
                        }
                        
                }
            }
            
            
            std::stringstream ss;
           
            std::map<std::string,int>::iterator iter;
            for(iter = sumMap.begin(); iter!=sumMap.end(); iter++) {
                ss << iter->first << " " << (iter->second/count) << "\n";
            }
            context.emit(key,ss.str());
        }
        
        
};

int main(int argc, char *argv[]) {
        return HadoopPipes::runTask(HadoopPipes::TemplateFactory<StripesMapper, StripesReducer >());
}
