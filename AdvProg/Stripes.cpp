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
					if(i==j) {
						continue;
					}
					std::string t = numbers[j];
					int count = 1.0;
					if(outer.find(t)!=outer.end()) {
						count = outer[t]+1;
					}
					outer[t]=count;
					inner.clear();
					inner[s] = 1;
					context.emit(t,mapToStr(inner));
				}
				context.emit(s,mapToStr(outer));
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
        StripesReducer( HadoopPipes::TaskContext& context ) {
        }
        
        void reduce(HadoopPipes::ReduceContext& context) {
            	std::string key = context.getInputKey();
		double marginals = 0.0;
		std::map<std::string, double> out;
		while(context.nextValue()) {
			std::string strMap = context.getInputValue();
			std::vector<std::string> pairs = HadoopUtils::splitString(strMap,";");
			std::map<std::string, int> in;
			for(int i=0; i<pairs.size(); i++) {
				std::vector<std::string> pair = HadoopUtils::splitString(pairs[i],",");
				std::string k = pair[0].erase(0,1);
				std::string v = pair[1].erase(pair[1].size()-1,1);
				int vInt = HadoopUtils::toInt(v);
				in[k] = vInt;
			}
			std::map<std::string, int>::iterator it;
			for(it=in.begin(); it!=in.end(); ++it) {
				double tmp = (1.0*it->second);
				marginals+=tmp;
				std::string k = it->first;
				if(out.find(k)!=out.end()) {
					tmp += out[k];
				}
				out[k] = tmp;
			}
		}
		std::map<std::string, double> probs;
		std::map<std::string, double>::iterator it;
		for(it=out.begin(); it!=out.end(); ++it) {
			double c = it->second;
			double prob = c/marginals;
			probs[it->first] = prob;
		}
		std::stringstream ss;
		std::map<std::string, double>::iterator iter;
		for(iter=probs.begin(); iter!=probs.end(); ++iter) {
			ss << iter->first << ": " << iter->second << "\n";
		}
		context.emit(key,ss.str());
        }
        
        
};

int main(int argc, char *argv[]) {
        return HadoopPipes::runTask(HadoopPipes::TemplateFactory<StripesMapper, StripesReducer >());
}
