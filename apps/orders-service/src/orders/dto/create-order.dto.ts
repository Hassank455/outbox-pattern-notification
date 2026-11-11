import { IsEmail, IsInt, IsNotEmpty, IsString, Max, Min } from 'class-validator';

export class CreateOrderDto {
  @IsEmail()
  customerEmail!: string;

  @IsString()
  @IsNotEmpty()
  product!: string;

  @IsInt()
  @Min(1)
  @Max(1000)
  quantity!: number;

  @IsInt()
  @Min(1)
  unitPriceCents!: number;
}
