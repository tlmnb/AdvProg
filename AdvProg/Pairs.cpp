#include <algorithm>
#include <limits>
#include <string>
#include <sstream>

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"

class PairsMapper : public HadoopPipes::Mapper {
    public:
        PairsMapper( HadoopPipes::TaskContext& context ) {
        }
    
    void map( HadoopPipes::MapContext& context ) {
        std::string line = context.getInputValue();
        std::vector<std::string> numbers = HadoopUtils::splitString(line, " ");
        
        for(int i=0; i<numbers.size(); i++) {
            for(int j=0; j<numbers.size(); j++) {
                if(i==j) { continue; }
                context.emit(numbers[i]+",*","1");
                context.emit(numbers[i]+","+numbers[j], "1");
            }
        }
    }

};


class PairsPartitioner : public HadoopPipes::Partitioner {
    public:
        PairsPartitioner(HadoopPipes::TaskContext& context) {
        }
    
        int partition(const std::string& key, int numOfReduces) {
            std::vector<std::string> parts = HadoopUtils::splitString(key,",");
            int x = HadoopUtils::toInt(parts[0]);
            return x % numOfReduces;
        }
};


class PairsReducer : public HadoopPipes::Reducer {
    private:
        int marginal;
    public:
        PairsReducer(HadoopPipes::TaskContext& context) { 
            (*this).marginal = 0;
        }
        
        
        
    void reduce(HadoopPipes::ReduceContext& context) {
        std::string key = context.getInputKey();
        std::vector<std::string> parts = HadoopUtils::splitString(key,",");
        if(parts[1]=="*") {
            int sum=0;
            while(context.nextValue()) {
                sum+=HadoopUtils::toInt(context.getInputValue());
            }
            (*this).marginal = sum;
        } else {
            int sum=0;
            while(context.nextValue()) {
                sum+=HadoopUtils::toInt(context.getInputValue());
            }
            double relFreq = (1.0*sum) / (1.0*(*this).marginal);
            std::stringstream ss;
            ss << relFreq;
            context.emit(key,ss.str());
        }
    }
    
};

int main(int argc, char *argv[]) {
        return HadoopPipes::runTask(HadoopPipes::TemplateFactory<PairsMapper, PairsReducer, PairsPartitioner >());
}