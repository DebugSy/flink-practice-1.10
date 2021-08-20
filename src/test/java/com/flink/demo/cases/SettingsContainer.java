package com.flink.demo.cases;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by P0007 on 2019/12/23.
 *
 * 标识该类包含其他配置
 *
 * 注意：如果需要使用scope属性绑定参数作用域，使用 {@link SettingsContainer}
 *      修饰的，scope需要带上前缀, 如{@code scope = "quantifier.strategy"}
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(value = ElementType.TYPE)
public @interface SettingsContainer {
}