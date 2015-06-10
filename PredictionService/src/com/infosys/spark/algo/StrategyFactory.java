package com.infosys.spark.algo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class StrategyFactory 
{
	private static final StrategyFactory strategyFactory = new StrategyFactory();

	private Map<ClassificationAlgorithmEnum, ClassificationAlgorithmStrategy> strategyInstanceMap = new ConcurrentHashMap<ClassificationAlgorithmEnum, ClassificationAlgorithmStrategy>();

	private StrategyFactory() 
	{
		ClassificationAlgorithmStrategy strategy = null;
		ClassificationAlgorithmEnum[] algoEnums = ClassificationAlgorithmEnum.values();
		for (int i = 0; i < algoEnums.length; i++) 
		{
			ClassificationAlgorithmEnum enumValue = algoEnums[i];
			switch (enumValue) 
			{
			case GLM:
				strategy = new GLMStrategy();
				strategyInstanceMap.put(ClassificationAlgorithmEnum.GLM, strategy);
				break;
			case GBM:
				strategy = new GBMStrategy();
				strategyInstanceMap.put(ClassificationAlgorithmEnum.GLM, strategy);
				break;
			case DEEP_LEARNING:
				strategy = new DeepLearningStragegy();
				strategyInstanceMap.put(ClassificationAlgorithmEnum.GLM, strategy);
				break;
			default:
				break;

			}
		}
	}

	public static StrategyFactory getInstance() {
		return strategyFactory;
	}

	public ClassificationAlgorithmStrategy getStrategyInstance(ClassificationAlgorithmEnum classificationAlgorithmEnum) 
	{
		return  strategyInstanceMap.get(classificationAlgorithmEnum);
	}
}
