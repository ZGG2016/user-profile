
use profile_tags;
DROP TABLE IF EXISTS `tbl_dim_products`;
CREATE TABLE `tbl_dim_products` (
                                    `id` int DEFAULT NULL,
                                    `name` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `tbl_dim_products` VALUES (1,'Haier/海尔冰箱');
INSERT INTO `tbl_dim_products` VALUES (2,'LED电视');
INSERT INTO `tbl_dim_products` VALUES (3,'Leader/统帅冰箱');
INSERT INTO `tbl_dim_products` VALUES (4,'冰吧');
INSERT INTO `tbl_dim_products` VALUES (5,'冷柜');
INSERT INTO `tbl_dim_products` VALUES (6,'净水机');
INSERT INTO `tbl_dim_products` VALUES (7,'前置过滤器');
INSERT INTO `tbl_dim_products` VALUES (8,'取暖电器');
INSERT INTO `tbl_dim_products` VALUES (9,'吸尘器/除螨仪');
INSERT INTO `tbl_dim_products` VALUES (10,'嵌入式厨电');

DROP TABLE IF EXISTS `tbl_dim_colors`;
CREATE TABLE `tbl_dim_colors` (
                                  `id` int DEFAULT NULL,
                                  `name` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `tbl_dim_colors` VALUES (1,'香槟金色');
INSERT INTO `tbl_dim_colors` VALUES (2,'黑色');
INSERT INTO `tbl_dim_colors` VALUES (3,'白色');
INSERT INTO `tbl_dim_colors` VALUES (4,'梦境极光【卡其金】');
INSERT INTO `tbl_dim_colors` VALUES (5,'梦境极光【布朗灰】');
INSERT INTO `tbl_dim_colors` VALUES (6,'粉色');
INSERT INTO `tbl_dim_colors` VALUES (7,'金属灰');
INSERT INTO `tbl_dim_colors` VALUES (8,'金色');
INSERT INTO `tbl_dim_colors` VALUES (9,'乐享金');
INSERT INTO `tbl_dim_colors` VALUES (10,'布鲁纲');